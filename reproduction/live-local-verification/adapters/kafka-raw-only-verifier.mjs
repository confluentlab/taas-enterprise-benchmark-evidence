#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const [fixtureRoot, bundleRoot, runId, integrationId, kafkaContainer = "flowplane-kafka", kafkaBootstrap = "kafka:9092", errorContract = "simulation"] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, runId, integrationId].every(Boolean)) {
  throw new Error("usage: kafka-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> <runId> <integrationId> [kafkaContainer] [kafkaBootstrap]");
}

const safeIntegration = integrationId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const safeRun = runId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const topic = (suffix) => `flowplane.${safeIntegration}.evidence.${safeRun}.${suffix}`;
const topics = { raw: topic("raw"), transformed: topic("transformed"), dlq: topic("dlq") };
const groupId = `flowplane-${safeIntegration}-evidence-${safeRun}`;
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl"));
const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json")));
const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));

function runDocker(args, options = {}) {
  const result = spawnSync("docker", args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`docker ${args.join(" ")} failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
  }
  return result.stdout;
}

function sortJson(value) {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]));
  return value;
}
function canonical(value) { return JSON.stringify(sortJson(value)); }
function sha256(value) { return crypto.createHash("sha256").update(value, "utf8").digest("hex"); }
function delay(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

function latestCount(name) {
  const output = runDocker(["exec", kafkaContainer, "kafka-get-offsets", "--bootstrap-server", kafkaBootstrap, "--topic", name, "--time", "-1"]);
  return output.split(/\r?\n/).filter(Boolean).reduce((total, line) => total + Number.parseInt(line.split(":").at(-1), 10), 0);
}

async function waitForCount(name, expected, timeoutMs = 900_000) {
  const started = Date.now();
  const observations = [];
  while (Date.now() - started < timeoutMs) {
    const count = latestCount(name);
    observations.push({ elapsedMs: Date.now() - started, count });
    if (count >= expected) return { count, observations };
    await delay(2_000);
  }
  const count = latestCount(name);
  throw new Error(`Timed out waiting for ${expected} records on ${name}; observed ${count}`);
}

function consume(name, count, consumerGroup) {
  const output = runDocker([
    "exec", kafkaContainer, "kafka-console-consumer",
    "--bootstrap-server", kafkaBootstrap,
    "--topic", name,
    "--from-beginning",
    "--group", consumerGroup,
    "--max-messages", String(count),
    "--timeout-ms", "60000",
  ]);
  const rows = output.split(/\r?\n/).filter((line) => line.trim()).map((line) => JSON.parse(line));
  if (rows.length !== count) throw new Error(`Expected ${count} records from ${name}, consumed ${rows.length}`);
  return rows;
}

function multiset(values) {
  const result = new Map();
  for (const value of values) result.set(value, (result.get(value) ?? 0) + 1);
  return result;
}
function equalMultiset(left, right) {
  if (left.size !== right.size) return false;
  for (const [key, count] of left) if (right.get(key) !== count) return false;
  return true;
}

const expectedOutputs = simulation.records.map((record) => record.outputPreview);
const expectedOutputHashes = multiset(expectedOutputs.map((value) => sha256(canonical(value))));
const simulationErrorCodes = [...new Set(invalidSimulation.errors.map((error) => {
  const match = String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/);
  if (!match) throw new Error(`Could not extract expected error code from: ${error}`);
  return match[1];
}))].sort();
const expectedErrorCodes = errorContract === "runtime-contract" ? ["VALIDATION_FAILED"] : simulationErrorCodes;

// The verifier has exactly one producer operation. Its target is hard-bound to the raw topic.
const rawInput = [...valid, ...invalid];
runDocker([
  "exec", "-i", kafkaContainer, "kafka-console-producer",
  "--bootstrap-server", kafkaBootstrap,
  "--topic", topics.raw,
], { input: `${rawInput.map((record) => JSON.stringify(record)).join("\n")}\n` });

const transformedWait = await waitForCount(topics.transformed, valid.length);
const dlqWait = await waitForCount(topics.dlq, invalid.length);
const transformed = consume(topics.transformed, valid.length, `verify-transformed-${safeRun}`);
const errors = consume(topics.dlq, invalid.length, `verify-dlq-${safeRun}`);

const actualOutputHashes = multiset(transformed.map((value) => sha256(canonical(value))));
const expectedHashMatches = equalMultiset(expectedOutputHashes, actualOutputHashes) ? valid.length : 0;
let expectedErrorMatches = 0;
for (const value of errors) {
  const codes = [...new Set((value.errors ?? []).map((error) => error.code))].sort();
  if (canonical(codes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1;
}

let finalLag = 0;
let groupDescription = "";
try {
  groupDescription = runDocker(["exec", kafkaContainer, "kafka-consumer-groups", "--bootstrap-server", kafkaBootstrap, "--describe", "--group", groupId]);
  for (const line of groupDescription.split(/\r?\n/)) {
    const columns = line.trim().split(/\s+/);
    if (columns.length >= 6 && /^\d+$/.test(columns[5])) finalLag += Number.parseInt(columns[5], 10);
  }
} catch (error) {
  groupDescription = String(error);
  finalLag = Math.max(0, rawInput.length - transformed.length - errors.length);
}

const actualRoot = path.join(bundleRoot, "actual");
const metricsRoot = path.join(bundleRoot, "metrics");
fs.mkdirSync(actualRoot, { recursive: true });
fs.mkdirSync(metricsRoot, { recursive: true });
fs.writeFileSync(path.join(actualRoot, "transformed-output.jsonl"), `${transformed.map((value) => JSON.stringify(value)).join("\n")}\n`);
fs.writeFileSync(path.join(actualRoot, "error-output.jsonl"), `${errors.map((value) => JSON.stringify(value)).join("\n")}\n`);
fs.writeFileSync(path.join(metricsRoot, "kafka-consumer-group.txt"), `${groupDescription.trim()}\n`);
fs.writeFileSync(path.join(metricsRoot, "kafka-topic-counts.json"), `${JSON.stringify({ raw: latestCount(topics.raw), transformed: latestCount(topics.transformed), dlq: latestCount(topics.dlq), transformedWait, dlqWait }, null, 2)}\n`);

const report = {
  schemaVersion: "flowplane.kafka-raw-only-verification-result.v1",
  runId,
  integrationId,
  topics,
  groupId,
  boundary: "raw-only verifier producer -> integration consumer -> Flowplane sidecar -> integration transformed/DLQ producers -> verifier consumers",
  verifierWriteTargets: [topics.raw],
  verifierReadTargets: [topics.transformed, topics.dlq],
  attemptedInput: rawInput.length,
  acceptedInput: rawInput.length,
  validInput: valid.length,
  intentionalInvalid: invalid.length,
  successfulOutput: transformed.length,
  errorOutput: errors.length,
  filtered: 0,
  duplicates: transformed.length - new Set(transformed.map((value) => sha256(canonical(value)))).size,
  expectedHashMatches,
  expectedErrorMatches,
  expectedErrorCodes,
  errorContract,
  simulationErrorCodes,
  finalLag,
};
fs.writeFileSync(path.join(actualRoot, "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
