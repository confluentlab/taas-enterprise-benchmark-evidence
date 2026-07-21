#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { AckPolicy, DeliverPolicy, connect, JSONCodec } from "nats";

const [fixtureRoot, bundleRoot, server, runId] = process.argv.slice(2);
if (![fixtureRoot, bundleRoot, server, runId].every(Boolean)) throw new Error("usage: nats-jetstream-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> <server> <runId>");
const subject = (suffix) => `flowplane.nats.${runId.toLowerCase()}.${suffix}`;
const subjects = { raw: subject("raw"), transformed: subject("transformed"), dlq: subject("dlq") };
const stream = (suffix) => `FP_${runId.replace(/[^A-Za-z0-9]/g, "_").toUpperCase()}_${suffix}`;
const streams = { raw: stream("RAW"), transformed: stream("TRANSFORMED"), dlq: stream("DLQ") };
const readText = (file) => fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
const readJsonl = (file) => readText(file).split(/\r?\n/).filter(Boolean).map(JSON.parse);
const valid = readJsonl(path.join(fixtureRoot, "valid-input.jsonl"));
const invalid = readJsonl(path.join(fixtureRoot, "invalid-input.jsonl"));
const simulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-batch.json")));
const invalidSimulation = JSON.parse(readText(path.join(bundleRoot, "expected", "simulation-invalid.json")));
const expectedById = new Map(simulation.records.map((record) => [record.recordId, record.outputPreview]));
const expectedErrorCodes = [...new Set(invalidSimulation.errors.map((error) => { const match = String(error).match(/^[^:]+:\s+([A-Z0-9_]+)\s+-/); if (!match) throw new Error(`Cannot extract expected error code: ${error}`); return match[1]; }))].sort();
const sortJson = (value) => Array.isArray(value) ? value.map(sortJson) : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])])) : value;
const canonical = (value) => JSON.stringify(sortJson(value));
const sha256 = (value) => crypto.createHash("sha256").update(value, "utf8").digest("hex");
const jc = JSONCodec();
const nc = await connect({ servers: server, name: `raw-only-verifier-${runId}`, timeout: 30_000, maxReconnectAttempts: 10 });
const js = nc.jetstream();
const jsm = await nc.jetstreamManager();
const outputDurable = `verify_output_${runId.replace(/[^A-Za-z0-9]/g, "_")}`;
const dlqDurable = `verify_dlq_${runId.replace(/[^A-Za-z0-9]/g, "_")}`;
await jsm.consumers.add(streams.transformed, { durable_name: outputDurable, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, filter_subject: subjects.transformed });
await jsm.consumers.add(streams.dlq, { durable_name: dlqDurable, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, filter_subject: subjects.dlq });
const outputConsumer = await js.consumers.get(streams.transformed, outputDurable);
const dlqConsumer = await js.consumers.get(streams.dlq, dlqDurable);
const rawInput = [...valid.map((payload) => ({ payload, recordId: payload.event.id, kind: "valid", runId, publishedBy: "raw-only-verifier" })), ...invalid.map((payload) => ({ payload, recordId: payload.recordId, kind: "invalid", runId, publishedBy: "raw-only-verifier" }))];
const started = Date.now();
// The verifier's only publication target is the raw JetStream subject.
for (const record of rawInput) await js.publish(subjects.raw, jc.encode(record));
const receive = async (consumer, count) => { const rows = []; for (let i = 0; i < count; i += 1) { const message = await consumer.next({ expires: 90_000 }); if (!message) throw new Error(`JetStream verifier timed out after ${i}/${count}`); rows.push(message.json()); message.ack(); } return rows; };
const actualSuccess = await receive(outputConsumer, valid.length);
const actualErrors = await receive(dlqConsumer, invalid.length);
await nc.flush();
const seenSuccess = new Set(); let duplicates = 0; let expectedHashMatches = 0; const outputHashRows = [];
for (const record of actualSuccess) { if (seenSuccess.has(record.recordId)) duplicates += 1; seenSuccess.add(record.recordId); const actualHash = sha256(canonical(record.payload)); const expectedHash = sha256(canonical(expectedById.get(record.recordId))); if (actualHash === expectedHash) expectedHashMatches += 1; outputHashRows.push({ recordId: record.recordId, expectedHash, actualHash, matched: actualHash === expectedHash }); }
const seenErrors = new Set(); let expectedErrorMatches = 0;
for (const record of actualErrors) { if (seenErrors.has(record.recordId)) duplicates += 1; seenErrors.add(record.recordId); const codes = [...new Set((record.payload?.errors ?? []).map((error) => error.code))].sort(); if (canonical(codes) === canonical(expectedErrorCodes)) expectedErrorMatches += 1; }
fs.writeFileSync(path.join(bundleRoot, "actual", "transformed-output.jsonl"), `${actualSuccess.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "error-output.jsonl"), `${actualErrors.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(path.join(bundleRoot, "actual", "output-hashes.json"), `${JSON.stringify(outputHashRows, null, 2)}\n`);
const report = { schemaVersion: "flowplane.nats-jetstream-raw-only-verification-result.v1", runId, subjects, streams, boundary: "raw-only verifier -> JetStream raw -> independent pipeline -> Flowplane sidecar -> JetStream output/DLQ -> read-only durable consumers", verifierWriteTargets: [subjects.raw], verifierReadTargets: [subjects.transformed, subjects.dlq], attemptedInput: rawInput.length, acceptedInput: rawInput.length, validInput: valid.length, intentionalInvalid: invalid.length, successfulOutput: actualSuccess.length, errorOutput: actualErrors.length, filtered: 0, duplicates, expectedHashMatches, expectedErrorMatches, expectedErrorCodes, durationSeconds: Math.round((Date.now() - started) / 10) / 100 };
fs.writeFileSync(path.join(bundleRoot, "actual", "bridge-result.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report));
await nc.drain();
