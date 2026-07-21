#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import { Kafka, logLevel } from "kafkajs";

const [kafkaBootstrap, endpoint, mode, integrationId, runId, expectedText = "110", evidenceRoot = "/evidence", protoPath = "/app/flowplane_runtime.proto"] = process.argv.slice(2);
const modes = new Set(["http-single", "http-batch", "grpc-batch", "grpc-streaming", "aws-lambda", "azure-functions", "gcp-functions"]);
if (![kafkaBootstrap, endpoint, mode, integrationId, runId].every(Boolean) || !modes.has(mode)) {
  throw new Error("usage: kafka-runtime-surface-bridge.mjs <kafkaBootstrap> <endpoint> <mode> <integrationId> <runId> [expected] [evidenceRoot] [protoPath]");
}

const safeIntegration = integrationId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const safeRun = runId.toLowerCase().replace(/[^a-z0-9-]/g, "-");
const prefix = `flowplane.${safeIntegration}.evidence.${safeRun}`;
const topics = { raw: `${prefix}.raw`, transformed: `${prefix}.transformed`, dlq: `${prefix}.dlq` };
const expected = Number.parseInt(expectedText, 10);
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });

const runtimeStatus = JSON.parse(fs.readFileSync(path.join(actualRoot, "runtime-status-before.json"), "utf8"));
const metadata = {
  tenantId: "acme-corp",
  runtimeId: runtimeStatus.runtimeId,
  mappingId: runtimeStatus.mappingId ?? runtimeStatus.artifact?.mappingId ?? runtimeStatus.activeMappingId,
  mappingVersion: runtimeStatus.version ?? runtimeStatus.mappingVersion ?? runtimeStatus.artifact?.version,
  artifactHash: runtimeStatus.artifactHash ?? runtimeStatus.artifact?.hash,
};
for (const [name, value] of Object.entries(metadata)) {
  if (!value && !mode.endsWith("functions") && mode !== "aws-lambda") throw new Error(`runtime status is missing ${name}`);
}

const kafka = new Kafka({ clientId: `flowplane-${safeIntegration}-surface-${safeRun}`, brokers: [kafkaBootstrap], logLevel: logLevel.INFO });
const producer = kafka.producer({ allowAutoTopicCreation: false, idempotent: true, maxInFlightRequests: 1 });
const consumer = kafka.consumer({ groupId: `flowplane-${safeIntegration}-evidence-${safeRun}` });
const state = { received: 0, transformed: 0, dlq: 0, publishFailures: 0, runtimeFailures: 0, completed: false };

function writeJson(name, value) {
  fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
}

function recordId(payload, fallback) {
  const candidate = payload?.event?.id ?? payload?.recordId;
  return candidate == null || String(candidate).trim() === "" ? String(fallback) : String(candidate);
}

function sidecarBatch(records) {
  return {
    ...metadata,
    inputFormat: "JSON",
    outputFormat: "JSON",
    errorMode: "PER_RECORD",
    records: records.map(({ id, payload }) => ({ recordId: id, headers: {}, payloadJson: JSON.stringify(payload) })),
  };
}

function grpcBatch(records) {
  return {
    tenantId: metadata.tenantId,
    runtimeId: metadata.runtimeId,
    mappingId: metadata.mappingId,
    mappingVersion: String(metadata.mappingVersion),
    artifactHash: metadata.artifactHash,
    inputFormat: "JSON",
    outputFormat: "OUTPUT_JSON",
    errorMode: "PER_RECORD",
    records: records.map(({ id, payload }) => ({ recordId: id, headers: {}, payload: Buffer.from(JSON.stringify(payload)) })),
  };
}

async function fetchJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  let json;
  try { json = JSON.parse(text); } catch { throw new Error(`runtime returned non-JSON HTTP ${response.status}: ${text.slice(0, 200)}`); }
  return { status: response.status, json };
}

let grpcClient;
let grpcStream;
const streamPending = new Map();
if (mode.startsWith("grpc-")) {
  const definition = protoLoader.loadSync(protoPath, { keepCase: false, longs: String, enums: String, defaults: true, oneofs: true });
  const root = grpc.loadPackageDefinition(definition);
  const Service = root.flowplane.runtime.v1.FlowPlaneTransformService;
  grpcClient = new Service(endpoint, grpc.credentials.createInsecure());
  if (mode === "grpc-streaming") {
    grpcStream = grpcClient.TransformStream();
    grpcStream.on("data", (response) => {
      const pending = streamPending.get(response.batchId);
      if (pending) { streamPending.delete(response.batchId); pending.resolve(response.batch); }
    });
    grpcStream.on("error", (error) => {
      for (const pending of streamPending.values()) pending.reject(error);
      streamPending.clear();
    });
  }
}

function unaryGrpc(request) {
  return new Promise((resolve, reject) => grpcClient.TransformBatch(request, (error, response) => error ? reject(error) : resolve(response)));
}

function streamingGrpc(request, batchId) {
  return new Promise((resolve, reject) => {
    streamPending.set(batchId, { resolve, reject });
    grpcStream.write({ batchId, batch: request });
  });
}

function normalizeProtocolResults(results) {
  return results.map((result) => {
    const status = String(result.status ?? "");
    if (status === "OK") {
      const bytes = Buffer.isBuffer(result.output) ? result.output : Buffer.from(result.outputBase64 ?? "", "base64");
      return { id: result.recordId, destination: "transformed", value: JSON.parse(bytes.toString("utf8")) };
    }
    if (status === "DLQ") {
      const error = result.error ?? {};
      return { id: result.recordId, destination: "dlq", value: { errors: [{ code: error.code, message: error.message, fieldPath: error.fieldPath, stage: error.stage }] } };
    }
    throw new Error(`unexpected runtime result status ${status} for ${result.recordId}`);
  });
}

async function invoke(records, sequence) {
  if (mode === "http-single") {
    const output = [];
    for (const record of records) {
      const request = { ...sidecarBatch([record]), ...sidecarBatch([record]).records[0] };
      delete request.records;
      const response = await fetchJson(endpoint, request);
      if (response.status !== 200) throw new Error(`HTTP single returned ${response.status}: ${JSON.stringify(response.json)}`);
      output.push(...normalizeProtocolResults(response.json.results));
    }
    return output;
  }
  if (mode === "http-batch") {
    const response = await fetchJson(endpoint, sidecarBatch(records));
    if (response.status !== 200) throw new Error(`HTTP batch returned ${response.status}: ${JSON.stringify(response.json)}`);
    return normalizeProtocolResults(response.json.results);
  }
  if (mode === "grpc-batch") return normalizeProtocolResults((await unaryGrpc(grpcBatch(records))).results);
  if (mode === "grpc-streaming") return normalizeProtocolResults((await streamingGrpc(grpcBatch(records), `batch-${sequence}`)).results);

  const output = [];
  for (const record of records) {
    let response;
    if (mode === "aws-lambda") {
      const event = { body: JSON.stringify(record.payload), isBase64Encoded: false, requestContext: { apiId: "local-evidence", requestId: record.id } };
      const outer = await fetchJson(endpoint, event);
      if (outer.status !== 200) throw new Error(`Lambda RIE returned ${outer.status}`);
      response = { status: Number(outer.json.statusCode), json: JSON.parse(outer.json.body) };
    } else {
      response = await fetchJson(endpoint, record.payload);
    }
    if (response.status === 200) output.push({ id: record.id, destination: "transformed", value: response.json });
    else if (response.status === 422) output.push({ id: record.id, destination: "dlq", value: response.json });
    else throw new Error(`${mode} returned ${response.status}: ${JSON.stringify(response.json)}`);
  }
  return output;
}

async function publish(results) {
  for (const result of results) {
    try {
      await producer.send({ topic: topics[result.destination], acks: -1, messages: [{ key: result.id, value: JSON.stringify(result.value) }] });
      state[result.destination] += 1;
      state.received += 1;
    } catch (error) {
      state.publishFailures += 1;
      throw error;
    }
  }
  state.completed = state.received >= expected;
  writeJson("publisher-bridge-result.json", {
    schemaVersion: "flowplane.kafka-runtime-surface-bridge-result.v1",
    integrationId, runId, mode, endpoint, metadata, topics,
    boundary: "persistent Kafka raw input -> deployed runtime surface -> acknowledged Kafka transformed/DLQ output",
    ...state,
    updatedAt: new Date().toISOString(),
  });
}

await producer.connect();
await consumer.connect();
await consumer.subscribe({ topic: topics.raw, fromBeginning: true });
writeJson("publisher-bridge-ready.json", { integrationId, runId, mode, endpoint, metadata, topics, readyAt: new Date().toISOString() });

let sequence = 0;
await consumer.run({
  autoCommit: false,
  eachBatchAutoResolve: false,
  eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
    const size = mode === "http-batch" || mode.startsWith("grpc-") ? 10 : 1;
    for (let index = 0; index < batch.messages.length; index += size) {
      const slice = batch.messages.slice(index, index + size);
      const records = slice.map((message, offset) => {
        const payload = JSON.parse(message.value.toString("utf8"));
        return { id: recordId(payload, `${batch.partition}-${message.offset}-${offset}`), payload };
      });
      try {
        const results = await invoke(records, ++sequence);
        await publish(results);
      } catch (error) {
        state.runtimeFailures += records.length;
        writeJson("publisher-bridge-result.json", { integrationId, runId, mode, endpoint, metadata, topics, ...state, error: String(error?.stack ?? error) });
        throw error;
      }
      for (const message of slice) resolveOffset(message.offset);
      const lastOffset = slice.at(-1).offset;
      await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: String(BigInt(lastOffset) + 1n) }]);
      await heartbeat();
    }
    if (state.completed) {
      if (grpcStream) grpcStream.end();
      setTimeout(async () => { await consumer.disconnect(); await producer.disconnect(); process.exit(0); }, 500);
    }
  },
});
