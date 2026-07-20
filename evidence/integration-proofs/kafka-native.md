# Kafka and native runtime proofs

Focused reruns passed for Kafka Connect with MongoDB and PostgreSQL, Kafka Streams, Flink, and Spring Boot. The 30-minute Kafka/Flink soak separately demonstrated exact accounting at 1,080,001 inputs with final lag zero. The Kafka Connect S3 path did not pass because connector creation returned HTTP 500.

These runs validate local configurations and fixtures only. They do not certify every connector version, broker distribution, cloud network, schema, or failure mode.
