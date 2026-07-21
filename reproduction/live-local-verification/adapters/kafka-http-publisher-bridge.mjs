#!/usr/bin/env node
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { Kafka, logLevel } from "kafkajs";

const [kafkaBootstrap, runtimeUrl, integrationId, runId, expectedText = "110", evidenceRoot = "/evidence", sourceTopic = ""] = process.argv.slice(2);
if (![kafkaBootstrap, runtimeUrl, integrationId, runId].every(Boolean)) {
  throw new Error("usage: kafka-http-publisher-bridge.mjs <kafkaBootstrap> <runtimeUrl> <integrationId> <runId> [expected] [evidenceRoot]");
}

const safeIntegration = integrationId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const safeRun = runId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const prefix = `flowplane.${safeIntegration}.evidence.${safeRun}`;
const topics = { transformed: `${prefix}.transformed`, dlq: `${prefix}.dlq` };
const transformedTopic = topics.transformed;
const dlqTopic = topics.dlq;
const expected = Number.parseInt(expectedText, 10);
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });

const kafka = new Kafka({ clientId: `flowplane-${safeIntegration}-publisher-${safeRun}`, brokers: [kafkaBootstrap], logLevel: logLevel.INFO });
const producer = kafka.producer({ allowAutoTopicCreation: false, idempotent: true, maxInFlightRequests: 1 });
const consumer = sourceTopic ? kafka.consumer({ groupId: `flowplane-${safeIntegration}-publisher-${safeRun}` }) : null;
const state = { received: 0, transformed: 0, dlq: 0, publishFailures: 0, runtimeFailures: 0, completed: false };

function writeJson(name, value) {
  fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
}

function decodeOtlpBody(body) {
  const records = [];
  for (const resource of body?.resourceLogs ?? []) {
    for (const scope of resource.scopeLogs ?? []) {
      for (const record of scope.logRecords ?? []) {
        const value = record?.body?.stringValue ?? record?.body?.bytesValue;
        if (typeof value !== "string") throw new Error("OTLP log record body must contain stringValue or bytesValue");
        records.push(JSON.parse(value));
      }
    }
  }
  return records;
}

function extractPayloads(body) {
  if (Array.isArray(body?.resourceLogs)) return decodeOtlpBody(body);
  if (Array.isArray(body)) return body;
  return [body];
}

function normalizePayload(payload) {
  const after = payload?.payload?.after ?? payload?.after;
  if (after && typeof after.payload_json === "string") return JSON.parse(after.payload_json);
  return payload;
}

async function transformAndPublish(payload) {
  const recordId = payload?.event?.id || payload?.recordId || `record-${state.received + 1}`;
  const response = await fetch(runtimeUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-flowplane-source-topic": `${prefix}.raw`,
      "x-flowplane-source-key": String(recordId),
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30_000),
  });
  const responseText = await response.text();
  let output;
  try { output = JSON.parse(responseText); } catch { throw new Error(`Flowplane returned non-JSON HTTP ${response.status}`); }
  const destination = response.status === 200 ? "transformed" : response.status === 422 ? "dlq" : null;
  const destinationTopic = response.status === 200 ? transformedTopic : response.status === 422 ? dlqTopic : null;
  if (!destination) {
    state.runtimeFailures += 1;
    throw new Error(`Unexpected Flowplane HTTP ${response.status} for ${recordId}`);
  }
  try {
    await producer.send({ topic: destinationTopic, acks: -1, messages: [{ key: String(recordId), value: JSON.stringify(output) }] });
  } catch (error) {
    state.publishFailures += 1;
    throw error;
  }
  state.received += 1;
  state[destination] += 1;
  state.completed = state.received === expected;
  writeJson("publisher-bridge-result.json", {
    schemaVersion: "flowplane.kafka-http-publisher-bridge-result.v1",
    integrationId,
    runId,
    boundary: "integration HTTP delivery -> Flowplane runtime -> acknowledged Kafka transformed/DLQ publish",
    topics,
    ...state,
    updatedAt: new Date().toISOString(),
  });
}

await producer.connect();
if (consumer) {
  await consumer.connect();
  await consumer.subscribe({ topic: sourceTopic, fromBeginning: true });
  consumer.run({
    eachMessage: async ({ message }) => {
      const payload = normalizePayload(JSON.parse(message.value.toString("utf8")));
      await transformAndPublish(payload);
    },
  }).catch((error) => { console.error(error); process.exitCode = 1; });
}
writeJson("publisher-bridge-ready.json", { integrationId, runId, kafkaBootstrap, runtimeUrl, sourceTopic: sourceTopic || null, topics, readyAt: new Date().toISOString() });

const server = http.createServer(async (request, response) => {
  if (request.method === "GET" && request.url === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "UP", ...state }));
    return;
  }
  if (request.method !== "POST" || !["/ingest", "/v1/logs"].includes(request.url)) {
    response.writeHead(404).end();
    return;
  }
  const chunks = [];
  let size = 0;
  request.on("data", (chunk) => {
    size += chunk.length;
    if (size > 16 * 1024 * 1024) request.destroy(new Error("request body too large"));
    chunks.push(chunk);
  });
  request.on("end", async () => {
    try {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      for (const payload of extractPayloads(body)) await transformAndPublish(payload);
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
    } catch (error) {
      response.writeHead(502, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: String(error?.message ?? error) }));
    }
  });
});

server.listen(8090, "0.0.0.0", () => console.log(JSON.stringify({ event: "ready", port: 8090, topics })));
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.on(signal, () => server.close(async () => { if (consumer) await consumer.disconnect(); await producer.disconnect(); process.exit(0); }));
}
