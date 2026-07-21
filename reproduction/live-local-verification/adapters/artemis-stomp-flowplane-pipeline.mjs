#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import stompit from "stompit";

const [brokerHost, user, password, runtimeUrl, runId, expectedText, evidenceRoot] = process.argv.slice(2);
if (![brokerHost, user, password, runtimeUrl, runId, expectedText, evidenceRoot].every(Boolean)) {
  throw new Error("usage: artemis-stomp-flowplane-pipeline.mjs <host:port> <user> <password> <runtime> <run-id> <expected> <evidence>");
}
const [host, portText] = brokerHost.split(":");
const expected = Number.parseInt(expectedText, 10);
const prefix = `flowplane.artemis.${runId.toLowerCase()}`;
const queues = { raw: `${prefix}.raw`, transformed: `${prefix}.transformed`, dlq: `${prefix}.dlq` };
const actualRoot = path.join(evidenceRoot, "actual");
fs.mkdirSync(actualRoot, { recursive: true });
const writeJson = (name, value) => fs.writeFileSync(path.join(actualRoot, name), `${JSON.stringify(value, null, 2)}\n`);
const connect = () => new Promise((resolve, reject) => stompit.connect({
  host,
  port: Number.parseInt(portText, 10),
  connectHeaders: { host: "/", login: user, passcode: password, "heart-beat": "5000,5000" },
}, (error, client) => error ? reject(error) : resolve(client)));
const sendConfirmed = (client, destination, value) => new Promise((resolve, reject) => {
  const frame = client.send({ destination, "content-type": "application/json", persistent: "true" }, { onReceipt: resolve, onError: reject });
  frame.end(JSON.stringify(value));
});
const ackConfirmed = (client, message) => new Promise((resolve, reject) => {
  client.ack(message, {}, { onReceipt: resolve, onError: reject });
});
const disconnect = async (client) => client.destroy();

const receiver = await connect();
const publisher = await connect();
let successfulOutput = 0;
let errorOutput = 0;
let unexpectedFailures = 0;
let httpTimeouts = 0;
let connectionErrors = 0;
const httpStatusCounts = {};
let processing = Promise.resolve();
let completeResolve;
let completeReject;
const complete = new Promise((resolve, reject) => { completeResolve = resolve; completeReject = reject; });
const started = Date.now();

receiver.subscribe({ destination: queues.raw, id: "raw", ack: "client-individual", "consumer-window-size": "0" }, (error, message) => {
  if (error) { completeReject(error); return; }
  message.readString("utf8", (readError, text) => {
    if (readError) { completeReject(readError); return; }
    processing = processing.then(async () => {
      const envelope = JSON.parse(text);
      const payload = envelope.payload;
      const recordId = envelope.recordId ?? payload?.recordId ?? payload?.event?.id;
      let response;
      try {
        response = await fetch(runtimeUrl, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-flowplane-source-topic": queues.raw,
            "x-flowplane-source-partition": "0",
            "x-flowplane-source-offset": String(successfulOutput + errorOutput),
            "x-flowplane-source-key": recordId,
          },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(30_000),
        });
      } catch (fetchError) {
        if (fetchError?.name === "TimeoutError") httpTimeouts += 1;
        else connectionErrors += 1;
        throw fetchError;
      }
      httpStatusCounts[response.status] = (httpStatusCounts[response.status] ?? 0) + 1;
      const bodyText = await response.text();
      const body = JSON.parse(bodyText);
      let target;
      if (response.status === 200) target = queues.transformed;
      else if (response.status === 422) target = queues.dlq;
      else { unexpectedFailures += 1; throw new Error(`Unexpected Flowplane HTTP ${response.status}: ${bodyText.slice(0, 500)}`); }
      await sendConfirmed(publisher, target, { recordId, runId, payload: body, publishedBy: "artemis-stomp-flowplane-pipeline" });
      await ackConfirmed(receiver, message);
      if (response.status === 200) successfulOutput += 1;
      else errorOutput += 1;
      if (successfulOutput + errorOutput === expected) completeResolve();
    }).catch(completeReject);
  });
});

writeJson("pipeline-ready.json", {
  schemaVersion: "flowplane.artemis-stomp-pipeline-ready.v1",
  runId,
  ready: true,
  readTargets: [queues.raw],
  writeTargets: [queues.transformed, queues.dlq],
  acknowledgementOrder: "Flowplane HTTP -> downstream STOMP SEND receipt -> raw STOMP ACK receipt",
  runtimeUrl,
  readyAt: new Date().toISOString(),
});
console.log(JSON.stringify({ event: "pipeline-ready", queues }));

await Promise.race([complete, new Promise((_, reject) => setTimeout(() => reject(new Error("Artemis pipeline timeout")), 120_000))]);
await processing;
const result = {
  schemaVersion: "flowplane.artemis-stomp-pipeline-result.v1",
  runId,
  component: "independent Artemis STOMP-to-Flowplane pipeline",
  boundary: "Artemis raw client-individual subscription -> Flowplane HTTP -> receipted transformed/DLQ SEND -> receipted raw ACK",
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
await disconnect(receiver);
await disconnect(publisher);
process.exit(0);
