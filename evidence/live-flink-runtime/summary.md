# Live Flink runtime: 1 MiB local proof

The 2026-07-18 local Docker probe sent 1,000 measurement records plus one flush record through Kafka and the Flink runtime using four Kafka partitions and a 40 records/s target.

The latency run produced 1,001 outputs and zero DLQ records. Runtime transform latency was p50 2.512 ms, p95 11.438 ms, p99 17.294 ms, and max 92.099 ms. The allocation run also produced 1,001 outputs and zero DLQ records, with p50/p95/p99 allocation of 303,768 / 315,848 / 318,032 bytes and a 4,896,280-byte maximum outlier.

This is a local end-to-end runtime proof, not a core JMH result or a managed-service certification. Host scheduling, Kafka, Docker, Flink, serialization, and the harness influence the distribution.
