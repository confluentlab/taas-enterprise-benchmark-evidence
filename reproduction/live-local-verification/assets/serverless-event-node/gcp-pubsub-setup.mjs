import { PubSub } from "@google-cloud/pubsub";
const [projectId, ...topicNames] = process.argv.slice(2);
const pubsub = new PubSub({ projectId });
for (const name of topicNames) {
  const topic = pubsub.topic(name);
  if (!(await topic.exists())[0]) await pubsub.createTopic(name);
}
console.log(JSON.stringify({ projectId, topics: topicNames }));
