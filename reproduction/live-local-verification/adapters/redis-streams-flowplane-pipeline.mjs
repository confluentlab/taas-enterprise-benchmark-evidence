#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { createClient } from "redis";

const [redisUrl, runtimeUrl, runId, expectedCountText, evidenceRoot] = process.argv.slice(2);
if (![redisUrl, runtimeUrl, runId, expectedCountText, evidenceRoot].every(Boolean)) throw new Error("usage: redis-streams-flowplane-pipeline.mjs <redisUrl> <runtimeUrl> <runId> <expectedCount> <evidenceRoot>");
const expectedCount = Number.parseInt(expectedCountText, 10);
const stream = (suffix) => `flowplane:redis:${runId.toLowerCase()}:${suffix}`;
const streams = { raw: stream("raw"), transformed: stream("transformed"), dlq: stream("dlq") };
const group = `pipeline-${runId.toLowerCase()}`;
const consumerName = `pipeline-worker-${runId.toLowerCase()}`;
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });
const writeJson = (name, value) => fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
const redis = createClient({ url: redisUrl });
redis.on("error", (error) => console.error(`redis-error ${error.message}`));
await redis.connect();
try { await redis.xGroupCreate(streams.raw, group, "0", { MKSTREAM: true }); } catch (error) { if (!String(error).includes("BUSYGROUP")) throw error; }
writeJson("pipeline-ready.json", { schemaVersion: "flowplane.redis-streams-pipeline-ready.v1", runId, ready: true, readTargets: [streams.raw], writeTargets: [streams.transformed, streams.dlq], consumerGroup: group, runtimeUrl, readyAt: new Date().toISOString() });
console.log(JSON.stringify({ event: "pipeline-ready", runId, streams, group }));

const started = Date.now(); let successfulOutput = 0; let errorOutput = 0; let unexpectedFailures = 0; let httpTimeouts = 0; let connectionErrors = 0; const httpStatusCounts = {};
for (let index = 0; index < expectedCount; index += 1) {
  const batches = await redis.xReadGroup(group, consumerName, [{ key: streams.raw, id: ">" }], { COUNT: 1, BLOCK: 90_000 });
  const message = batches?.[0]?.messages?.[0];
  if (!message) throw new Error(`Redis raw consumer timed out after ${index} records`);
  const envelope = JSON.parse(message.message.data);
  const payload = envelope.payload;
  const recordId = envelope.recordId ?? payload?.recordId ?? payload?.event?.id;
  let response;
  try { response = await fetch(runtimeUrl, { method: "POST", headers: { "content-type": "application/json", "x-flowplane-source-topic": streams.raw, "x-flowplane-source-partition": "0", "x-flowplane-source-offset": String(index), "x-flowplane-source-key": recordId, "x-flowplane-redis-stream-id": message.id }, body: JSON.stringify(payload), signal: AbortSignal.timeout(30_000) }); }
  catch (error) { if (error?.name === "TimeoutError") httpTimeouts += 1; else connectionErrors += 1; throw error; }
  httpStatusCounts[response.status] = (httpStatusCounts[response.status] ?? 0) + 1;
  const bodyText = await response.text(); const body = JSON.parse(bodyText);
  if (response.status === 200) { await redis.xAdd(streams.transformed, "*", { data: JSON.stringify({ recordId, kind: "success", runId, payload: body, publishedBy: "redis-streams-flowplane-pipeline" }) }); successfulOutput += 1; }
  else if (response.status === 422) { await redis.xAdd(streams.dlq, "*", { data: JSON.stringify({ recordId, kind: "intentional-invalid", runId, payload: body, publishedBy: "redis-streams-flowplane-pipeline" }) }); errorOutput += 1; }
  else { unexpectedFailures += 1; throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}: ${bodyText.slice(0, 500)}`); }
  await redis.xAck(streams.raw, group, message.id);
}
const result = { schemaVersion: "flowplane.redis-streams-pipeline-result.v1", runId, component: "independently deployed Redis Streams-to-Flowplane pipeline container", boundary: "Redis raw consumer group -> Flowplane sidecar HTTP -> Redis transformed/DLQ XADD", readTargets: [streams.raw], writeTargets: [streams.transformed, streams.dlq], consumerGroup: group, processedInput: successfulOutput + errorOutput, successfulOutput, errorOutput, unexpectedFailures, httpTimeouts, connectionErrors, httpStatusCounts, durationSeconds: Math.round((Date.now() - started) / 10) / 100 };
writeJson("pipeline-result.json", result); console.log(JSON.stringify(result)); await redis.quit();
