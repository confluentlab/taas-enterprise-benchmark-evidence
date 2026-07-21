#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { Producer, SimpleConsumer } from "rocketmq-client-nodejs";

const [endpoints, runtimeUrl, runId, expectedText, evidenceRoot] = process.argv.slice(2);
if (![endpoints, runtimeUrl, runId, expectedText, evidenceRoot].every(Boolean)) {
  throw new Error("usage: rocketmq-flowplane-pipeline.mjs <proxy-endpoints> <runtime-url> <run-id> <expected> <evidence-root>");
}

const expected = Number.parseInt(expectedText, 10);
const prefix = `flowplane_rocketmq_${runId.toLowerCase()}`;
const topics = { raw: `${prefix}_raw`, transformed: `${prefix}_transformed`, dlq: `${prefix}_dlq` };
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });
const writeJson = (name, value) => fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);

const producer = new Producer({ endpoints });
const consumer = new SimpleConsumer({
  endpoints,
  consumerGroup: `flowplane-rocketmq-pipeline-${runId}`,
  subscriptions: new Map([[topics.raw, "*"]]),
  requestTimeout: 30_000,
  awaitDuration: 5_000,
});

let successfulOutput = 0;
let errorOutput = 0;
let unexpectedFailures = 0;
let httpTimeouts = 0;
let connectionErrors = 0;
const httpStatusCounts = {};
const started = Date.now();

await producer.startup();
await consumer.startup();
writeJson("pipeline-ready.json", {
  schemaVersion: "flowplane.rocketmq-pipeline-ready.v1",
  runId,
  ready: true,
  readTargets: [topics.raw],
  writeTargets: [topics.transformed, topics.dlq],
  acknowledgementOrder: "Flowplane HTTP -> confirmed RocketMQ downstream send -> raw ack",
  runtimeUrl,
  readyAt: new Date().toISOString(),
});
console.log(JSON.stringify({ event: "pipeline-ready", topics }));

const deadline = Date.now() + 120_000;
while (successfulOutput + errorOutput < expected && Date.now() < deadline) {
  const messages = await consumer.receive(Math.min(32, expected - successfulOutput - errorOutput), 60_000);
  for (const message of messages) {
    const envelope = JSON.parse(message.body.toString("utf8"));
    const payload = envelope.payload;
    const recordId = envelope.recordId ?? payload?.recordId ?? payload?.event?.id;
    let response;
    try {
      response = await fetch(runtimeUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-flowplane-source-topic": topics.raw,
          "x-flowplane-source-partition": "0",
          "x-flowplane-source-offset": String(message.offset ?? successfulOutput + errorOutput),
          "x-flowplane-source-key": recordId,
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(30_000),
      });
    } catch (error) {
      if (error?.name === "TimeoutError") httpTimeouts += 1;
      else connectionErrors += 1;
      throw error;
    }

    httpStatusCounts[response.status] = (httpStatusCounts[response.status] ?? 0) + 1;
    const bodyText = await response.text();
    const body = JSON.parse(bodyText);
    let target;
    if (response.status === 200) {
      target = topics.transformed;
    } else if (response.status === 422) {
      target = topics.dlq;
    } else {
      unexpectedFailures += 1;
      throw new Error(`Unexpected Flowplane HTTP ${response.status}: ${bodyText.slice(0, 500)}`);
    }

    await producer.send({
      topic: target,
      tag: response.status === 200 ? "transformed" : "dlq",
      keys: [recordId],
      body: Buffer.from(JSON.stringify({ recordId, runId, payload: body, publishedBy: "rocketmq-flowplane-pipeline" })),
    });
    await consumer.ack(message);
    if (response.status === 200) successfulOutput += 1;
    else errorOutput += 1;
  }
}

if (successfulOutput + errorOutput !== expected) {
  throw new Error(`RocketMQ pipeline timeout: processed=${successfulOutput + errorOutput}, expected=${expected}`);
}

const result = {
  schemaVersion: "flowplane.rocketmq-pipeline-result.v1",
  runId,
  component: "independent RocketMQ-to-Flowplane pipeline",
  boundary: "RocketMQ raw SimpleConsumer -> Flowplane HTTP -> confirmed RocketMQ transformed/DLQ Producer.send -> raw ack",
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
await consumer.shutdown();
await producer.shutdown();
