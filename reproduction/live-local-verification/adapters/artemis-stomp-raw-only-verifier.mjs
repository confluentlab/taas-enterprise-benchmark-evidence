#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import stompit from "stompit";

const [fixtureRoot, bundleRoot, brokerHost, user, password, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, brokerHost, user, password, runId].every(Boolean)) {
  throw new Error("usage: artemis-stomp-raw-only-verifier.mjs <fixtures> <bundle> <host:port> <user> <password> <run-id>");
}
const [host, portText] = brokerHost.split(":");
const prefix = `flowplane.artemis.${runId.toLowerCase()}`;
const queues = { raw: `${prefix}.raw`, transformed: `${prefix}.transformed`, dlq: `${prefix}.dlq` };
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map(JSON.parse);
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl"));
const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json")));
const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));
const expectedById = new Map(simulation.records.map((record) => [record.recordId, record.outputPreview]));
const expectedErrorCodes = [...new Set(invalidSimulation.errors.map((error) => {
  const match = String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/);
  if (!match) throw new Error(`No error code in: ${error}`);
  return match[1];
}))].sort();
const sortJson = (value) => Array.isArray(value) ? value.map(sortJson) : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])])) : value;
const canonical = (value) => JSON.stringify(sortJson(value));
const sha256 = (value) => crypto.createHash("sha256").update(value, "utf8").digest("hex");
const connect = () => new Promise((resolve, reject) => stompit.connect({
  host,
  port: Number.parseInt(portText, 10),
  connectHeaders: { host: "/", login: user, passcode: password, "heart-beat": "5000,5000" },
}, (error, client) => error ? reject(error) : resolve(client)));
const sendConfirmed = (client, destination, value) => new Promise((resolve, reject) => {
  const frame = client.send({ destination, "content-type": "application/json", persistent: "true" }, { onReceipt: resolve, onError: reject });
  frame.end(JSON.stringify(value));
});
const ackConfirmed = (client, message) => new Promise((resolve, reject) => client.ack(message, {}, { onReceipt: resolve, onError: reject }));
const disconnect = async (client) => client.destroy();

const receiver = await connect();
const producer = await connect();
const successes = [];
const errors = [];
let receiveChain = Promise.resolve();
let completeResolve;
let completeReject;
const complete = new Promise((resolve, reject) => { completeResolve = resolve; completeReject = reject; });
const onOutput = (kind) => (error, message) => {
  if (error) { completeReject(error); return; }
  message.readString("utf8", (readError, text) => {
    if (readError) { completeReject(readError); return; }
    receiveChain = receiveChain.then(async () => {
      const value = JSON.parse(text);
      if (kind === "transformed") successes.push(value);
      else errors.push(value);
      await ackConfirmed(receiver, message);
      if (successes.length === 100 && errors.length === 10) completeResolve();
    }).catch(completeReject);
  });
};
receiver.subscribe({ destination: queues.transformed, id: "transformed", ack: "client-individual" }, onOutput("transformed"));
receiver.subscribe({ destination: queues.dlq, id: "dlq", ack: "client-individual" }, onOutput("dlq"));
await new Promise((resolve) => setTimeout(resolve, 500));

const raw = [
  ...valid.map((payload) => ({ payload, recordId: payload.event.id, kind: "valid", runId, publishedBy: "raw-only-verifier" })),
  ...invalid.map((payload) => ({ payload, recordId: payload.recordId, kind: "invalid", runId, publishedBy: "raw-only-verifier" })),
];
const started = Date.now();
for (const record of raw) await sendConfirmed(producer, queues.raw, record);
await Promise.race([complete, new Promise((_, reject) => setTimeout(() => reject(new Error(`Artemis output timeout success=${successes.length} errors=${errors.length}`)), 120_000))]);
await receiveChain;

let duplicates = 0;
let expectedHashMatches = 0;
let expectedErrorMatches = 0;
const seen = new Set();
const hashes = [];
for (const record of successes) {
  if (seen.has(record.recordId)) duplicates += 1;
  seen.add(record.recordId);
  const actualHash = sha256(canonical(record.payload));
  const expectedHash = sha256(canonical(expectedById.get(record.recordId)));
  if (actualHash === expectedHash) expectedHashMatches += 1;
  hashes.push({ recordId: record.recordId, expectedHash, actualHash, matched: actualHash === expectedHash });
}
for (const record of errors) {
  if (seen.has(record.recordId)) duplicates += 1;
  seen.add(record.recordId);
  const codes = [...new Set((record.payload?.errors ?? []).map((error) => error.code))].sort();
  if (canonical(codes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1;
}
fs.writeFileSync(path.join(bundleRoot, "actual", "transformed-output.jsonl"), `${successes.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "error-output.jsonl"), `${errors.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "output-hashes.json"), `${JSON.stringify(hashes, null, 2)}\n`);
const report = {
  schemaVersion: "flowplane.artemis-stomp-raw-only-verification-result.v1",
  runId,
  queues,
  verifierWriteTargets: [queues.raw],
  verifierReadTargets: [queues.transformed, queues.dlq],
  attemptedInput: 110,
  acceptedInput: 110,
  validInput: 100,
  intentionalInvalid: 10,
  successfulOutput: 100,
  errorOutput: 10,
  filtered: 0,
  duplicates,
  expectedHashMatches,
  expectedErrorMatches,
  expectedErrorCodes,
  durationSeconds: Math.round((Date.now() - started) / 10) / 100,
};
fs.writeFileSync(path.join(bundleRoot, "actual", "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
await disconnect(receiver);
await disconnect(producer);
process.exit(0);
