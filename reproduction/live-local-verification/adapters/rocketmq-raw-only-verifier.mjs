#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Producer, SimpleConsumer } from "rocketmq-client-nodejs";

const [fixtureRoot, bundleRoot, endpoints, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, endpoints, runId].every(Boolean)) {
  throw new Error("usage: rocketmq-raw-only-verifier.mjs <fixtures> <bundle> <proxy-endpoints> <run-id>");
}

const prefix = `flowplane_rocketmq_${runId.toLowerCase()}`;
const topics = { raw: `${prefix}_raw`, transformed: `${prefix}_transformed`, dlq: `${prefix}_dlq` };
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
const sortJson = (value) => Array.isArray(value)
  ? value.map(sortJson)
  : value && typeof value === "object"
    ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]))
    : value;
const canonical = (value) => JSON.stringify(sortJson(value));
const sha256 = (value) => crypto.createHash("sha256").update(value, "utf8").digest("hex");

const transformedConsumer = new SimpleConsumer({
  endpoints,
  consumerGroup: `flowplane-rocketmq-verifier-transformed-${runId}`,
  subscriptions: new Map([[topics.transformed, "*"]]),
  requestTimeout: 30_000,
  awaitDuration: 5_000,
});
const dlqConsumer = new SimpleConsumer({
  endpoints,
  consumerGroup: `flowplane-rocketmq-verifier-dlq-${runId}`,
  subscriptions: new Map([[topics.dlq, "*"]]),
  requestTimeout: 30_000,
  awaitDuration: 5_000,
});
const producer = new Producer({ endpoints });
await Promise.all([transformedConsumer.startup(), dlqConsumer.startup(), producer.startup()]);

const raw = [
  ...valid.map((payload) => ({ payload, recordId: payload.event.id, kind: "valid", runId, publishedBy: "raw-only-verifier" })),
  ...invalid.map((payload) => ({ payload, recordId: payload.recordId, kind: "invalid", runId, publishedBy: "raw-only-verifier" })),
];
const started = Date.now();
for (const record of raw) {
  await producer.send({ topic: topics.raw, tag: record.kind, keys: [record.recordId], body: Buffer.from(JSON.stringify(record)) });
}

const successes = [];
const errors = [];
const deadline = Date.now() + 120_000;
while ((successes.length < 100 || errors.length < 10) && Date.now() < deadline) {
  const [transformedMessages, dlqMessages] = await Promise.all([
    transformedConsumer.receive(32, 60_000),
    dlqConsumer.receive(10, 60_000),
  ]);
  for (const message of transformedMessages) {
    const value = JSON.parse(message.body.toString("utf8"));
    if (message.topic !== topics.transformed) throw new Error(`Unexpected transformed consumer topic: ${message.topic}`);
    successes.push(value);
    await transformedConsumer.ack(message);
  }
  for (const message of dlqMessages) {
    const value = JSON.parse(message.body.toString("utf8"));
    if (message.topic !== topics.dlq) throw new Error(`Unexpected DLQ consumer topic: ${message.topic}`);
    errors.push(value);
    await dlqConsumer.ack(message);
  }
}
if (successes.length !== 100 || errors.length !== 10) {
  throw new Error(`RocketMQ output timeout success=${successes.length} errors=${errors.length}`);
}

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
  schemaVersion: "flowplane.rocketmq-raw-only-verification-result.v1",
  runId,
  topics,
  verifierWriteTargets: [topics.raw],
  verifierReadTargets: [topics.transformed, topics.dlq],
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
await transformedConsumer.shutdown();
await dlqConsumer.shutdown();
await producer.shutdown();
