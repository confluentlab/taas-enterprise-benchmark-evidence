#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const [fixtureRoot, bundleRoot, runId, mysqlContainer, mysqlPassword, kafkaContainer = "flowplane-kafka", kafkaBootstrap = "kafka:9092"] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, runId, mysqlContainer, mysqlPassword].every(Boolean)) {
  throw new Error("usage: debezium-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> <runId> <mysqlContainer> <mysqlPassword> [kafkaContainer] [kafkaBootstrap]");
}

const safeRun = runId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const prefix = `flowplane.debezium.evidence.${safeRun}`;
const topics = { raw: `${prefix}.raw`, transformed: `${prefix}.transformed`, dlq: `${prefix}.dlq` };
const groupId = `flowplane-debezium-publisher-${safeRun}`;
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map(JSON.parse);
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl"));
const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json")));
const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));

function docker(args, options = {}) {
  const result = spawnSync("docker", args, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const safeArgs = args.map((arg) => String(arg).startsWith("MYSQL_PWD=") ? "MYSQL_PWD=<redacted>" : arg);
    fs.writeFileSync(path.join(bundleRoot, "actual", "verifier-docker-failure.json"), `${JSON.stringify({ status: result.status, args: safeArgs, stderr: result.stderr, stdout: result.stdout }, null, 2)}\n`);
    throw new Error(`Docker operation failed (${result.status}): ${(result.stderr || result.stdout || "").trim()}`);
  }
  return result.stdout;
}
function sortJson(value) {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]));
  return value;
}
const canonical = (value) => JSON.stringify(sortJson(value));
const sha256 = (value) => crypto.createHash("sha256").update(value, "utf8").digest("hex");
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function latestCount(topic) {
  const output = docker(["exec", kafkaContainer, "kafka-get-offsets", "--bootstrap-server", kafkaBootstrap, "--topic", topic, "--time", "-1"]);
  return output.split(/\r?\n/).filter(Boolean).reduce((sum, row) => sum + Number.parseInt(row.split(":").at(-1), 10), 0);
}
async function waitForCount(topic, expected, timeoutMs = 240_000) {
  const started = Date.now();
  const observations = [];
  while (Date.now() - started < timeoutMs) {
    const count = latestCount(topic);
    observations.push({ elapsedMs: Date.now() - started, count });
    if (count >= expected) return { count, observations };
    await delay(2_000);
  }
  throw new Error(`Timed out waiting for ${expected} records on ${topic}; observed ${latestCount(topic)}`);
}
function consume(topic, count, group) {
  const output = docker(["exec", kafkaContainer, "kafka-console-consumer", "--bootstrap-server", kafkaBootstrap, "--topic", topic, "--from-beginning", "--group", group, "--max-messages", String(count), "--timeout-ms", "60000"]);
  const rows = output.split(/\r?\n/).filter((line) => line.trim()).map(JSON.parse);
  if (rows.length !== count) throw new Error(`Expected ${count} records from ${topic}, consumed ${rows.length}`);
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

// The verifier's only write is this SQL INSERT into the persistent raw table.
const rawInput = [...valid, ...invalid];
const values = rawInput.map((record) => {
  const id = record.event?.id || record.recordId;
  const id64 = Buffer.from(String(id), "utf8").toString("base64");
  const json64 = Buffer.from(JSON.stringify(record), "utf8").toString("base64");
  return `(CAST(FROM_BASE64('${id64}') AS CHAR CHARACTER SET utf8mb4), CAST(FROM_BASE64('${json64}') AS CHAR CHARACTER SET utf8mb4))`;
});
const sql = `INSERT INTO records (record_id, payload_json) VALUES ${values.join(",")};\n`;
docker(["exec", "-i", "-e", `MYSQL_PWD=${mysqlPassword}`, mysqlContainer, "mysql", "-uroot", "flowplane"], { input: sql });

const rawWait = await waitForCount(topics.raw, rawInput.length);
const transformedWait = await waitForCount(topics.transformed, valid.length);
const dlqWait = await waitForCount(topics.dlq, invalid.length);
const transformed = consume(topics.transformed, valid.length, `verify-transformed-${safeRun}`);
const errors = consume(topics.dlq, invalid.length, `verify-dlq-${safeRun}`);
const expectedHashes = multiset(simulation.records.map((record) => sha256(canonical(record.outputPreview))));
const actualHashes = multiset(transformed.map((record) => sha256(canonical(record))));
const expectedHashMatches = equalMultiset(expectedHashes, actualHashes) ? valid.length : 0;
const expectedErrorCodes = [...new Set(invalidSimulation.errors.map((error) => String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/)?.[1]).filter(Boolean))].sort();
let expectedErrorMatches = 0;
for (const value of errors) {
  const codes = [...new Set((value.errors ?? []).map((error) => error.code))].sort();
  if (canonical(codes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1;
}
let finalLag = 0;
let groupDescription = "";
let lagRows = 0;
try {
  groupDescription = docker(["exec", kafkaContainer, "kafka-consumer-groups", "--bootstrap-server", kafkaBootstrap, "--describe", "--group", groupId]);
  for (const line of groupDescription.split(/\r?\n/)) {
    const columns = line.trim().split(/\s+/);
    if (columns.length >= 6 && /^\d+$/.test(columns[2]) && /^\d+$/.test(columns[5])) {
      lagRows += 1;
      finalLag += Number.parseInt(columns[5], 10);
    }
  }
  if (lagRows === 0) throw new Error("consumer-group description contained no partition rows");
} catch (error) { throw new Error(`Broker-derived consumer lag is mandatory; group inspection failed: ${String(error)}`); }

const actualRoot = path.join(bundleRoot, "actual");
const metricsRoot = path.join(bundleRoot, "metrics");
fs.writeFileSync(path.join(actualRoot, "transformed-output.jsonl"), `${transformed.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(actualRoot, "error-output.jsonl"), `${errors.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(metricsRoot, "kafka-consumer-group.txt"), `${groupDescription.trim()}\n`);
fs.writeFileSync(path.join(metricsRoot, "kafka-topic-counts.json"), `${JSON.stringify({ raw: latestCount(topics.raw), transformed: latestCount(topics.transformed), dlq: latestCount(topics.dlq), rawWait, transformedWait, dlqWait }, null, 2)}\n`);
const report = {
  schemaVersion: "flowplane.debezium-raw-only-verification-result.v1", runId, integrationId: "debezium", topics, groupId,
  boundary: "raw-only SQL INSERT -> MySQL binlog -> Debezium CDC topic -> Flowplane publisher sidecar -> Kafka transformed/DLQ topics",
  verifierWriteTargets: [`mysql://${mysqlContainer}/flowplane.records`], verifierReadTargets: [topics.transformed, topics.dlq],
  attemptedInput: rawInput.length, acceptedInput: rawInput.length, validInput: valid.length, intentionalInvalid: invalid.length,
  successfulOutput: transformed.length, errorOutput: errors.length, filtered: 0,
  duplicates: transformed.length - new Set(transformed.map((value) => value.eventId ?? value.recordId)).size
    + errors.length - new Set(errors.map((value) => value.recordId ?? value.source?.key ?? JSON.parse(value.payload?.snippet ?? "{}").recordId)).size,
  expectedHashMatches, expectedErrorMatches, expectedErrorCodes, finalLag,
};
fs.writeFileSync(path.join(actualRoot, "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
