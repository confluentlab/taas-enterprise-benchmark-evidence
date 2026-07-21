#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const [brokerBase, runtimeUrl, runId, expectedCountText, evidenceRoot] = process.argv.slice(2);
if (![brokerBase, runtimeUrl, runId, expectedCountText, evidenceRoot].every(Boolean)) {
  throw new Error("usage: activemq-classic-flowplane-pipeline.mjs <brokerBase> <runtimeUrl> <runId> <expectedCount> <evidenceRoot>");
}

const expectedCount = Number.parseInt(expectedCountText, 10);
if (!Number.isInteger(expectedCount) || expectedCount < 1) throw new Error(`Invalid expected count: ${expectedCountText}`);
const queue = (suffix) => `flowplane.activemq-classic.${runId.toLowerCase()}.${suffix}`;
const queues = { raw: queue("raw"), transformed: queue("transformed"), dlq: queue("dlq") };
const actualRoot = path.join(evidenceRoot, "actual");
const authorization = `Basic ${Buffer.from("admin:admin").toString("base64")}`;
fs.mkdirSync(actualRoot, { recursive: true });

function writeJson(name, value) {
  fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
}
function queueUrl(name, extra = "") {
  return `${brokerBase}/api/message/${encodeURIComponent(name)}?type=queue${extra}`;
}
async function receiveFromQueue(name) {
  const response = await fetch(queueUrl(name, `&clientId=pipeline-${encodeURIComponent(runId)}&readTimeout=90000`), {
    headers: { authorization, accept: "application/json" },
    signal: AbortSignal.timeout(95_000),
  });
  if (response.status !== 200) throw new Error(`ActiveMQ consume failed for ${name}: HTTP ${response.status} ${await response.text()}`);
  return JSON.parse(await response.text());
}
async function sendToQueue(name, value) {
  const response = await fetch(queueUrl(name), {
    method: "POST",
    headers: { authorization, "content-type": "application/json", persistent: "true" },
    body: JSON.stringify(value),
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`ActiveMQ publish failed for ${name}: HTTP ${response.status} ${await response.text()}`);
}

const started = Date.now();
let successfulOutput = 0;
let errorOutput = 0;
let unexpectedFailures = 0;
let httpTimeouts = 0;
let connectionErrors = 0;
const httpStatusCounts = {};

writeJson("pipeline-ready.json", {
  schemaVersion: "flowplane.activemq-classic-pipeline-ready.v1",
  runId,
  ready: true,
  readTargets: [queues.raw],
  writeTargets: [queues.transformed, queues.dlq],
  runtimeUrl,
  readyAt: new Date().toISOString(),
});
console.log(JSON.stringify({ event: "pipeline-ready", runId, queues }));

for (let index = 0; index < expectedCount; index += 1) {
  const message = await receiveFromQueue(queues.raw);
  const payload = message.payload;
  const recordId = message.recordId ?? payload?.recordId ?? payload?.event?.id;
  let response;
  try {
    response = await fetch(runtimeUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-flowplane-source-topic": queues.raw,
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
    await sendToQueue(queues.transformed, { recordId, kind: "success", runId, payload: body, publishedBy: "activemq-classic-flowplane-pipeline" });
    successfulOutput += 1;
  } else if (response.status === 422) {
    await sendToQueue(queues.dlq, { recordId, kind: "intentional-invalid", runId, payload: body, publishedBy: "activemq-classic-flowplane-pipeline" });
    errorOutput += 1;
  } else {
    unexpectedFailures += 1;
    throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}: ${bodyText.slice(0, 500)}`);
  }
}

const result = {
  schemaVersion: "flowplane.activemq-classic-pipeline-result.v1",
  runId,
  component: "independently deployed ActiveMQ Classic-to-Flowplane pipeline container",
  boundary: "ActiveMQ raw queue -> Flowplane sidecar HTTP -> ActiveMQ transformed/DLQ queues",
  readTargets: [queues.raw],
  writeTargets: [queues.transformed, queues.dlq],
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
