#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { AckPolicy, DeliverPolicy, RetentionPolicy, StorageType, connect, JSONCodec } from "nats";

const [server, runtimeUrl, runId, expectedCountText, evidenceRoot] = process.argv.slice(2);
if (![server, runtimeUrl, runId, expectedCountText, evidenceRoot].every(Boolean)) throw new Error("usage: nats-jetstream-flowplane-pipeline.mjs <server> <runtimeUrl> <runId> <expectedCount> <evidenceRoot>");
const expectedCount = Number.parseInt(expectedCountText, 10);
const safeRun = runId.toLowerCase();
const subject = (suffix) => `flowplane.nats.${safeRun}.${suffix}`;
const subjects = { raw: subject("raw"), transformed: subject("transformed"), dlq: subject("dlq") };
const stream = (suffix) => `FP_${runId.replace(/[^A-Za-z0-9]/g, "_").toUpperCase()}_${suffix}`;
const streams = { raw: stream("RAW"), transformed: stream("TRANSFORMED"), dlq: stream("DLQ") };
const durable = `pipeline_${runId.replace(/[^A-Za-z0-9]/g, "_")}`;
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });
const writeJson = (name, value) => fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
const jc = JSONCodec();
const nc = await connect({ servers: server, name: `flowplane-pipeline-${runId}`, timeout: 30_000, maxReconnectAttempts: 10 });
const js = nc.jetstream();
const jsm = await nc.jetstreamManager();

for (const key of Object.keys(streams)) {
  try {
    await jsm.streams.add({ name: streams[key], subjects: [subjects[key]], retention: RetentionPolicy.Limits, storage: StorageType.File, num_replicas: 1 });
  } catch (error) {
    if (!String(error).includes("stream name already in use")) throw error;
  }
}
await jsm.consumers.add(streams.raw, { durable_name: durable, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, filter_subject: subjects.raw, max_ack_pending: expectedCount });
const consumer = await js.consumers.get(streams.raw, durable);
writeJson("pipeline-ready.json", { schemaVersion: "flowplane.nats-jetstream-pipeline-ready.v1", runId, ready: true, readTargets: [subjects.raw], writeTargets: [subjects.transformed, subjects.dlq], streams, durableConsumer: durable, runtimeUrl, readyAt: new Date().toISOString() });
console.log(JSON.stringify({ event: "pipeline-ready", runId, subjects, streams, durable }));

const started = Date.now();
let successfulOutput = 0;
let errorOutput = 0;
let unexpectedFailures = 0;
let httpTimeouts = 0;
let connectionErrors = 0;
const httpStatusCounts = {};
for (let index = 0; index < expectedCount; index += 1) {
  const message = await consumer.next({ expires: 90_000 });
  if (!message) throw new Error(`JetStream raw consumer timed out after ${index} records`);
  const envelope = message.json();
  const payload = envelope.payload;
  const recordId = envelope.recordId ?? payload?.recordId ?? payload?.event?.id;
  let response;
  try {
    response = await fetch(runtimeUrl, { method: "POST", headers: { "content-type": "application/json", "x-flowplane-source-topic": subjects.raw, "x-flowplane-source-partition": "0", "x-flowplane-source-offset": String(message.seq), "x-flowplane-source-key": recordId }, body: JSON.stringify(payload), signal: AbortSignal.timeout(30_000) });
  } catch (error) {
    if (error?.name === "TimeoutError") httpTimeouts += 1; else connectionErrors += 1;
    throw error;
  }
  httpStatusCounts[response.status] = (httpStatusCounts[response.status] ?? 0) + 1;
  const bodyText = await response.text();
  const body = JSON.parse(bodyText);
  if (response.status === 200) {
    await js.publish(subjects.transformed, jc.encode({ recordId, kind: "success", runId, payload: body, publishedBy: "nats-jetstream-flowplane-pipeline" }));
    successfulOutput += 1;
  } else if (response.status === 422) {
    await js.publish(subjects.dlq, jc.encode({ recordId, kind: "intentional-invalid", runId, payload: body, publishedBy: "nats-jetstream-flowplane-pipeline" }));
    errorOutput += 1;
  } else {
    unexpectedFailures += 1;
    throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}: ${bodyText.slice(0, 500)}`);
  }
  message.ack();
}
await nc.flush();
const result = { schemaVersion: "flowplane.nats-jetstream-pipeline-result.v1", runId, component: "independently deployed NATS JetStream-to-Flowplane pipeline container", boundary: "JetStream raw durable consumer -> Flowplane sidecar HTTP -> JetStream transformed/DLQ publishers", readTargets: [subjects.raw], writeTargets: [subjects.transformed, subjects.dlq], streams, durableConsumer: durable, processedInput: successfulOutput + errorOutput, successfulOutput, errorOutput, unexpectedFailures, httpTimeouts, connectionErrors, httpStatusCounts, durationSeconds: Math.round((Date.now() - started) / 10) / 100 };
writeJson("pipeline-result.json", result);
console.log(JSON.stringify(result));
await nc.drain();
