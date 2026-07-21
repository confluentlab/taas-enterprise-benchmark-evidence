import http from "node:http";
const target = process.env.FUNCTION_TARGET_URL || "http://gcp-function:8080/";
http.createServer(async (request, response) => {
  try {
    const chunks = []; for await (const chunk of request) chunks.push(chunk);
    const envelope = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    const result = await fetch(target, { method: "POST", headers: { "content-type": "application/json", "ce-id": envelope.message?.messageId || crypto.randomUUID(), "ce-source": "//pubsub.googleapis.com/projects/flowplane-local/topics/raw", "ce-specversion": "1.0", "ce-type": "google.cloud.pubsub.topic.v1.messagePublished" }, body: JSON.stringify({ message: envelope.message, subscription: envelope.subscription }) });
    if (!result.ok) console.error(`function returned ${result.status}: ${await result.text()}`);
    else console.log(`forwarded ${envelope.message?.messageId || "unknown"}`);
    response.writeHead(result.ok ? 204 : 500); response.end();
  } catch (error) { console.error(error); response.writeHead(500); response.end(); }
}).listen(8080, "0.0.0.0", () => console.log("eventarc bridge ready"));
