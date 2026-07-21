#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const [fixtureRoot, bundleRoot, pulsarHttpBase, runtimeUrl, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, pulsarHttpBase, runtimeUrl, runId].every(Boolean)) {
  throw new Error("usage: pulsar-http-bridge.mjs <fixtureRoot> <bundleRoot> <pulsarHttpBase> <runtimeUrl> <runId>");
}

const topic = (suffix) => `flowplane-pulsar-${runId.toLowerCase()}-${suffix}`;
const topics = { raw: topic("raw"), transformed: topic("transformed"), dlq: topic("dlq") };
const wsBase = pulsarHttpBase.replace(/^http/, "ws");
const producerUrl = (name) => `${wsBase}/ws/v2/producer/persistent/public/default/${name}`;
const consumerUrl = (name, subscription) => `${wsBase}/ws/v2/consumer/persistent/public/default/${name}/${subscription}?subscriptionType=Exclusive&subscriptionInitialPosition=Earliest&receiverQueueSize=1000`;
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
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]));
  }
  return value;
}
function canonical(value) { return JSON.stringify(sortJson(value)); }
function sha256(value) { return crypto.createHash("sha256").update(value, "utf8").digest("hex"); }
function delay(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

class JsonSocket {
  constructor(url) {
    this.url = url;
    this.queue = [];
    this.waiters = [];
  }
  async open() {
    this.socket = new WebSocket(this.url);
    this.socket.addEventListener("message", (event) => {
      const value = JSON.parse(String(event.data));
      const waiter = this.waiters.shift();
      if (waiter) waiter.resolve(value); else this.queue.push(value);
    });
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(`WebSocket open timeout: ${this.url}`)), 30_000);
      this.socket.addEventListener("open", () => { clearTimeout(timeout); resolve(); }, { once: true });
      this.socket.addEventListener("error", () => { clearTimeout(timeout); reject(new Error(`WebSocket open failed: ${this.url}`)); }, { once: true });
    });
    return this;
  }
  next(timeoutMs = 60_000) {
    if (this.queue.length) return Promise.resolve(this.queue.shift());
    return new Promise((resolve, reject) => {
      const waiter = { resolve: (value) => { clearTimeout(timer); resolve(value); }, reject };
      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error(`WebSocket message timeout: ${this.url}`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  send(value) { this.socket.send(JSON.stringify(value)); }
  close() { try { this.socket.close(1000, "verification complete"); } catch {} }
}

async function sendRecord(producer, payload, recordId, kind) {
  producer.send({
    payload: Buffer.from(JSON.stringify(payload), "utf8").toString("base64"),
    properties: { recordId, kind, runId },
    key: recordId,
  });
  const response = await producer.next();
  if (response.result && response.result !== "ok") throw new Error(`Pulsar producer failed: ${JSON.stringify(response)}`);
  return response.messageId;
}

async function receiveRecord(consumer) {
  const message = await consumer.next();
  const payload = JSON.parse(Buffer.from(message.payload, "base64").toString("utf8"));
  consumer.send({ messageId: message.messageId });
  return { payload, properties: message.properties ?? {}, messageId: message.messageId };
}

const sockets = [];
const started = Date.now();
const rawInput = [...valid.map((payload) => ({ payload, recordId: payload.event.id, kind: "valid" })), ...invalid.map((payload) => ({ payload, recordId: payload.recordId, kind: "invalid" }))];
const actualSuccess = [];
const actualErrors = [];
let httpTimeouts = 0;
let connectionErrors = 0;
let unexpectedFailures = 0;

try {
  const rawConsumer = await new JsonSocket(consumerUrl(topics.raw, `bridge-${runId}`)).open(); sockets.push(rawConsumer);
  const outputConsumer = await new JsonSocket(consumerUrl(topics.transformed, `verify-output-${runId}`)).open(); sockets.push(outputConsumer);
  const dlqConsumer = await new JsonSocket(consumerUrl(topics.dlq, `verify-dlq-${runId}`)).open(); sockets.push(dlqConsumer);
  const rawProducer = await new JsonSocket(producerUrl(topics.raw)).open(); sockets.push(rawProducer);
  const outputProducer = await new JsonSocket(producerUrl(topics.transformed)).open(); sockets.push(outputProducer);
  const dlqProducer = await new JsonSocket(producerUrl(topics.dlq)).open(); sockets.push(dlqProducer);

  for (const record of rawInput) await sendRecord(rawProducer, record.payload, record.recordId, record.kind);

  for (let index = 0; index < rawInput.length; index += 1) {
    const source = await receiveRecord(rawConsumer);
    const recordId = source.properties.recordId ?? source.payload.recordId ?? source.payload.event?.id;
    let response;
    try {
      response = await fetch(runtimeUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-flowplane-source-topic": topics.raw,
          "x-flowplane-source-partition": "0",
          "x-flowplane-source-offset": String(index),
          "x-flowplane-source-key": recordId,
        },
        body: JSON.stringify(source.payload),
        signal: AbortSignal.timeout(30_000),
      });
    } catch (error) {
      if (error?.name === "TimeoutError") httpTimeouts += 1; else connectionErrors += 1;
      throw error;
    }
    const bodyText = await response.text();
    const body = JSON.parse(bodyText);
    if (response.status === 200) {
      await sendRecord(outputProducer, body, recordId, "success");
    } else if (response.status === 422) {
      await sendRecord(dlqProducer, body, recordId, "intentional-invalid");
    } else {
      unexpectedFailures += 1;
      throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}: ${bodyText.slice(0, 500)}`);
    }
  }

  for (let index = 0; index < valid.length; index += 1) actualSuccess.push(await receiveRecord(outputConsumer));
  for (let index = 0; index < invalid.length; index += 1) actualErrors.push(await receiveRecord(dlqConsumer));
  await delay(1_000);
} finally {
  for (const socket of sockets.reverse()) socket.close();
}

const seenSuccess = new Set();
let duplicates = 0;
let expectedHashMatches = 0;
const outputHashRows = [];
for (const record of actualSuccess) {
  const recordId = record.properties.recordId;
  if (seenSuccess.has(recordId)) duplicates += 1;
  seenSuccess.add(recordId);
  const actualCanonical = canonical(record.payload);
  const expectedCanonical = canonical(expectedById.get(recordId));
  const actualHash = sha256(actualCanonical);
  const expectedHash = sha256(expectedCanonical);
  if (actualHash === expectedHash) expectedHashMatches += 1;
  outputHashRows.push({ recordId, expectedHash, actualHash, matched: actualHash === expectedHash });
}
const seenErrors = new Set();
let expectedErrorMatches = 0;
for (const record of actualErrors) {
  const recordId = record.properties.recordId;
  if (seenErrors.has(recordId)) duplicates += 1;
  seenErrors.add(recordId);
  const actualErrorCodes = [...new Set((record.payload.errors ?? []).map((error) => error.code))].sort();
  if (canonical(actualErrorCodes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1;
}

fs.writeFileSync(path.join(bundleRoot, "actual", "transformed-output.jsonl"), `${actualSuccess.map((item) => JSON.stringify(item)).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "error-output.jsonl"), `${actualErrors.map((item) => JSON.stringify(item)).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "output-hashes.json"), `${JSON.stringify(outputHashRows, null, 2)}\n`);

const report = {
  schemaVersion: "flowplane.pulsar-http-bridge-result.v1",
  runId,
  topics,
  boundary: "Apache Pulsar WebSocket producer -> Pulsar subscription -> real Flowplane Bento HTTP -> Pulsar output/DLQ topics",
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
  unexpectedFailures,
  httpTimeouts,
  connectionErrors,
  durationSeconds: Math.round((Date.now() - started) / 10) / 100,
};
fs.writeFileSync(path.join(bundleRoot, "actual", "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
