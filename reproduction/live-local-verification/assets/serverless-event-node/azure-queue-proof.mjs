import fs from "node:fs";
import { QueueClient } from "@azure/storage-queue";

const [connection, inputName, outputName, dlqName, fixtureRoot, evidenceRoot] = process.argv.slice(2);
if (![connection, inputName, outputName, dlqName, fixtureRoot, evidenceRoot].every(Boolean)) throw new Error("missing arguments");
const rows = (name) => fs.readFileSync(`${fixtureRoot}/${name}`, "utf8").trim().split(/\r?\n/).filter(Boolean);
const input = new QueueClient(connection, inputName);
const output = new QueueClient(connection, outputName);
const dlq = new QueueClient(connection, dlqName);
for (const client of [input, output, dlq]) await client.createIfNotExists();
for (const payload of [...rows("valid-input.jsonl"), ...rows("invalid-input.jsonl")]) await input.sendMessage(Buffer.from(payload).toString("base64"));
async function drain(client, expected, timeoutMs = 300000) {
  const values = []; const started = Date.now();
  while (values.length < expected && Date.now() - started < timeoutMs) {
    const page = await client.receiveMessages({ numberOfMessages: Math.min(32, expected - values.length), visibilityTimeout: 30 });
    for (const message of page.receivedMessageItems) {
      values.push(JSON.parse(Buffer.from(message.messageText, "base64").toString("utf8")));
      await client.deleteMessage(message.messageId, message.popReceipt);
    }
    if (!page.receivedMessageItems.length) await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  if (values.length !== expected) throw new Error(`expected ${expected}, received ${values.length}`);
  return values;
}
const transformed = await drain(output, rows("valid-input.jsonl").length);
const errors = await drain(dlq, rows("invalid-input.jsonl").length);
fs.mkdirSync(`${evidenceRoot}/actual`, { recursive: true });
fs.writeFileSync(`${evidenceRoot}/actual/transformed-output.jsonl`, `${transformed.map(JSON.stringify).join("\n")}\n`);
fs.writeFileSync(`${evidenceRoot}/actual/error-output.jsonl`, `${errors.map(JSON.stringify).join("\n")}\n`);
const result = { trigger: "azure-queue", attemptedInput: transformed.length + errors.length, successfulOutput: transformed.length, errorOutput: errors.length, verifierWriteTargets: [inputName], verifierReadTargets: [outputName, dlqName] };
fs.writeFileSync(`${evidenceRoot}/actual/trigger-result.json`, `${JSON.stringify(result, null, 2)}\n`);
console.log(JSON.stringify(result));
