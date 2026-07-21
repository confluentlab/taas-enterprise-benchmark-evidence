#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const [pulsarHttpBase, runtimeUrl, runId, expectedCountText, evidenceRoot] = process.argv.slice(2);
if (![pulsarHttpBase, runtimeUrl, runId, expectedCountText, evidenceRoot].every(Boolean)) {
  throw new Error("usage: pulsar-flowplane-pipeline.mjs <pulsarHttpBase> <runtimeUrl> <runId> <expectedCount> <evidenceRoot>");
}

const expectedCount = Number.parseInt(expectedCountText, 10);
if (!Number.isInteger(expectedCount) || expectedCount < 1) throw new Error(`Invalid expected count: ${expectedCountText}`);

const topic = (suffix) => `flowplane-pulsar-${runId.toLowerCase()}-${suffix}`;
const topics = { raw: topic("raw"), transformed: topic("transformed"), dlq: topic("dlq") };
const wsBase = pulsarHttpBase.replace(/^http/, "ws");
const producerUrl = (name) => `${wsBase}/ws/v2/producer/persistent/public/default/${name}`;
const consumerUrl = (name, subscription) => `${wsBase}/ws/v2/consumer/persistent/public/default/${name}/${subscription}?subscriptionType=Exclusive&subscriptionInitialPosition=Earliest&receiverQueueSize=1000`;
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });

function writeJson(name, value) {
  fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
}
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
  close() { try { this.socket.close(1000, "pipeline complete"); } catch {} }
}

async function publish(producer, payload, recordId, kind) {
  producer.send({
    payload: Buffer.from(JSON.stringify(payload), "utf8").toString("base64"),
    properties: { recordId, kind, runId, publishedBy: "flowplane-pulsar-pipeline" },
    key: recordId,
  });
  const response = await producer.next();
  if (response.result && response.result !== "ok") throw new Error(`Pulsar producer failed: ${JSON.stringify(response)}`);
}

const sockets = [];
const started = Date.now();
let successfulOutput = 0;
let errorOutput = 0;
let unexpectedFailures = 0;
let httpTimeouts = 0;
let connectionErrors = 0;
const httpStatusCounts = {};

try {
  const rawConsumer = await new JsonSocket(consumerUrl(topics.raw, `pipeline-${runId}`)).open(); sockets.push(rawConsumer);
  const outputProducer = await new JsonSocket(producerUrl(topics.transformed)).open(); sockets.push(outputProducer);
  const dlqProducer = await new JsonSocket(producerUrl(topics.dlq)).open(); sockets.push(dlqProducer);

  writeJson("pipeline-ready.json", {
    schemaVersion: "flowplane.pulsar-pipeline-ready.v1",
    runId,
    ready: true,
    readTargets: [topics.raw],
    writeTargets: [topics.transformed, topics.dlq],
    runtimeUrl,
    readyAt: new Date().toISOString(),
  });
  console.log(JSON.stringify({ event: "pipeline-ready", runId, topics }));

  for (let index = 0; index < expectedCount; index += 1) {
    const message = await rawConsumer.next(90_000);
    const payload = JSON.parse(Buffer.from(message.payload, "base64").toString("utf8"));
    const recordId = message.properties?.recordId ?? payload.recordId ?? payload.event?.id;
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
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(30_000),
      });
    } catch (error) {
      if (error?.name === "TimeoutError") httpTimeouts += 1; else connectionErrors += 1;
      throw error;
    }

    httpStatusCounts[response.status] = (httpStatusCounts[response.status] ?? 0) + 1;
    const bodyText = await response.text();
    const body = JSON.parse(bodyText);
    if (response.status === 200) {
      await publish(outputProducer, body, recordId, "success");
      successfulOutput += 1;
    } else if (response.status === 422) {
      await publish(dlqProducer, body, recordId, "intentional-invalid");
      errorOutput += 1;
    } else {
      unexpectedFailures += 1;
      throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}: ${bodyText.slice(0, 500)}`);
    }

    // Acknowledge the raw record only after the downstream publish succeeds.
    rawConsumer.send({ messageId: message.messageId });
  }
  await delay(500);

  const result = {
    schemaVersion: "flowplane.pulsar-pipeline-result.v1",
    runId,
    component: "independently deployed Pulsar-to-Flowplane pipeline container",
    boundary: "Pulsar raw subscription -> Flowplane sidecar HTTP -> Pulsar transformed/DLQ producer",
    readTargets: [topics.raw],
    writeTargets: [topics.transformed, topics.dlq],
    processedInput: successfulOutput + errorOutput,
    successfulOutput,
    errorOutput,
    unexpectedFailures,
    httpTimeouts,
    connectionErrors,
    httpStatusCounts,
    durationSeconds: Math.round((Date.now() - started) / 10) / 100,
  };
  writeJson("pipeline-result.json", result);
  console.log(JSON.stringify(result));
} finally {
  for (const socket of sockets.reverse()) socket.close();
}
