#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const [fixtureRoot, bundleRoot, brokerBase, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, brokerBase, runId].every(Boolean)) {
  throw new Error("usage: activemq-classic-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> <brokerBase> <runId>");
}

const queue = (suffix) => `flowplane.activemq-classic.${runId.toLowerCase()}.${suffix}`;
const queues = { raw: queue("raw"), transformed: queue("transformed"), dlq: queue("dlq") };
const authorization = `Basic ${Buffer.from("admin:admin").toString("base64")}`;
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl"));
const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json")));
const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));
const expectedById = new Map(simulation.records.map((record) => [record.recordId, record.outputPreview]));
const expectedErrorCodes = [...new Set(invalidSimulation.errors.map((error) => {
  const match = String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/);
  if (!match) throw new Error(`Could not extract an error code from invalid simulation result: ${error}`);
  return match[1];
}))].sort();

function sortJson(value) {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]));
  return value;
}
function canonical(value) { return JSON.stringify(sortJson(value)); }
function sha256(value) { return crypto.createHash("sha256").update(value, "utf8").digest("hex"); }
function queueUrl(name, extra = "") { return `${brokerBase}/api/message/${encodeURIComponent(name)}?type=queue${extra}`; }
async function sendToQueue(name, value) {
  const response = await fetch(queueUrl(name), {
    method: "POST",
    headers: { authorization, "content-type": "application/json", persistent: "true" },
    body: JSON.stringify(value),
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`ActiveMQ raw publish failed: HTTP ${response.status} ${await response.text()}`);
}
async function receiveFromQueue(name, clientId) {
  const response = await fetch(queueUrl(name, `&clientId=${encodeURIComponent(clientId)}&readTimeout=90000`), {
    headers: { authorization, accept: "application/json" },
    signal: AbortSignal.timeout(95_000),
  });
  if (response.status !== 200) throw new Error(`ActiveMQ consume failed for ${name}: HTTP ${response.status} ${await response.text()}`);
  return JSON.parse(await response.text());
}

const started = Date.now();
const rawInput = [
  ...valid.map((payload) => ({ payload, recordId: payload.event.id, kind: "valid", runId, publishedBy: "raw-only-verifier" })),
  ...invalid.map((payload) => ({ payload, recordId: payload.recordId, kind: "invalid", runId, publishedBy: "raw-only-verifier" })),
];

// This is intentionally the verifier's only producer call and raw is its only write target.
for (const record of rawInput) await sendToQueue(queues.raw, record);
const actualSuccess = [];
const actualErrors = [];
for (let index = 0; index < valid.length; index += 1) actualSuccess.push(await receiveFromQueue(queues.transformed, `verify-output-${runId}`));
for (let index = 0; index < invalid.length; index += 1) actualErrors.push(await receiveFromQueue(queues.dlq, `verify-dlq-${runId}`));

const seenSuccess = new Set();
let duplicates = 0;
let expectedHashMatches = 0;
const outputHashRows = [];
for (const record of actualSuccess) {
  if (seenSuccess.has(record.recordId)) duplicates += 1;
  seenSuccess.add(record.recordId);
  const actualHash = sha256(canonical(record.payload));
  const expectedHash = sha256(canonical(expectedById.get(record.recordId)));
  if (actualHash === expectedHash) expectedHashMatches += 1;
  outputHashRows.push({ recordId: record.recordId, expectedHash, actualHash, matched: actualHash === expectedHash });
}

const seenErrors = new Set();
let expectedErrorMatches = 0;
for (const record of actualErrors) {
  if (seenErrors.has(record.recordId)) duplicates += 1;
  seenErrors.add(record.recordId);
  const actualErrorCodes = [...new Set((record.payload?.errors ?? []).map((error) => error.code))].sort();
  if (canonical(actualErrorCodes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1;
}

fs.writeFileSync(path.join(bundleRoot, "actual", "transformed-output.jsonl"), `${actualSuccess.map((item) => JSON.stringify(item)).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "error-output.jsonl"), `${actualErrors.map((item) => JSON.stringify(item)).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "output-hashes.json"), `${JSON.stringify(outputHashRows, null, 2)}\n`);
const report = {
  schemaVersion: "flowplane.activemq-classic-raw-only-verification-result.v1",
  runId,
  queues,
  boundary: "raw-only verifier -> ActiveMQ raw queue -> independent pipeline -> Flowplane sidecar -> ActiveMQ output/DLQ -> read-only verifier",
  verifierWriteTargets: [queues.raw],
  verifierReadTargets: [queues.transformed, queues.dlq],
  attemptedInput: rawInput.length,
  acceptedInput: rawInput.length,
  validInput: valid.length,
  intentionalInvalid: invalid.length,
  successfulOutput: actualSuccess.length,
  errorOutput: actualErrors.length,
  filtered: 0,
  duplicates,
  expectedHashMatches,
  expectedErrorMatches,
  expectedErrorCodes,
  durationSeconds: Math.round((Date.now() - started) / 10) / 100,
};
fs.writeFileSync(path.join(bundleRoot, "actual", "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
