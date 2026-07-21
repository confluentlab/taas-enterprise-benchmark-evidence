import fs from "node:fs";
import { EventHubConsumerClient, EventHubProducerClient, latestEventPosition } from "@azure/event-hubs";
import { QueueClient } from "@azure/storage-queue";

const [eventHubConnection, inputHub, outputHub, consumerGroup, storageConnection, dlqName, fixtureRoot, evidenceRoot] = process.argv.slice(2);
const rows = (name) => fs.readFileSync(`${fixtureRoot}/${name}`, "utf8").trim().split(/\r?\n/).filter(Boolean);
const valid = rows("valid-input.jsonl"), invalid = rows("invalid-input.jsonl");
const dlq = new QueueClient(storageConnection, dlqName); await dlq.createIfNotExists();
const received = [];
const consumer = new EventHubConsumerClient(consumerGroup, eventHubConnection, outputHub);
const subscription = consumer.subscribe({ processEvents: async (events) => { for (const event of events) received.push(typeof event.body === "string" ? JSON.parse(event.body) : event.body); }, processError: async (error) => console.error(error) }, { startPosition: latestEventPosition });
const producer = new EventHubProducerClient(eventHubConnection, inputHub);
let batch = await producer.createBatch();
for (const payload of [...valid, ...invalid]) { const event = { body: Buffer.from(payload, "utf8") }; if (!batch.tryAdd(event)) { await producer.sendBatch(batch); batch = await producer.createBatch(); if (!batch.tryAdd(event)) throw new Error("record too large"); } }
if (batch.count) await producer.sendBatch(batch);
const started = Date.now(); while (received.length < valid.length && Date.now() - started < 300000) await new Promise((resolve) => setTimeout(resolve, 1000));
const errors = [];
while (errors.length < invalid.length && Date.now() - started < 300000) { const page = await dlq.receiveMessages({ numberOfMessages: Math.min(32, invalid.length - errors.length), visibilityTimeout: 30 }); for (const message of page.receivedMessageItems) { errors.push(JSON.parse(Buffer.from(message.messageText, "base64").toString("utf8"))); await dlq.deleteMessage(message.messageId, message.popReceipt); } if (!page.receivedMessageItems.length) await new Promise((resolve) => setTimeout(resolve, 1000)); }
await subscription.close(); await consumer.close(); await producer.close();
if (received.length !== valid.length || errors.length !== invalid.length) throw new Error(`expected ${valid.length}/${invalid.length}, received ${received.length}/${errors.length}`);
fs.mkdirSync(`${evidenceRoot}/actual`, { recursive: true });
fs.writeFileSync(`${evidenceRoot}/actual/transformed-output.jsonl`, `${received.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(`${evidenceRoot}/actual/error-output.jsonl`, `${errors.map(JSON.stringify).join("\n")}\n`);
const result = { trigger: "azure-eventhub", attemptedInput: received.length + errors.length, successfulOutput: received.length, errorOutput: errors.length, verifierWriteTargets: [inputHub], verifierReadTargets: [outputHub, dlqName] };
fs.writeFileSync(`${evidenceRoot}/actual/trigger-result.json`, `${JSON.stringify(result, null, 2)}\n`); console.log(JSON.stringify(result));
