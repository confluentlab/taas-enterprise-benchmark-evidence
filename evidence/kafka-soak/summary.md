# Kafka/Flink 30-minute soak

**Status: `LIVE_LOCAL_VERIFIED`.** The live local run met its completion and exact-accounting criteria.

The local Docker soak targeted 600 records/s for 1,800 seconds using 102,400-byte payloads and a deliberate 1% missing-tenant failure path.

| Measure | Result |
|---|---:|
| Configured target | 600 records/s for 1,800 seconds |
| Produced | 1,080,001 records |
| Producer failures | 0 |
| Raw input payload volume | 110,591,022,399 bytes; approximately 103.0 GiB (before Kafka framing and replication) |
| Observed produced rate | 599.876 records/s; 58.581 MiB/s |
| Successful outputs | 1,069,201 |
| Intentional failures | 10,800 |
| Final raw-topic lag | 0 |
| Transform latency | p50 779 µs; p95 7,124 µs; p99 15,596 µs; p99.9 32,031 µs |
| Observed completed rate | 532.145 records/s; 51.967 MiB/s |
| Drain duration | 2,029.525 seconds |
| GC | 1,491 young collections / 2,700 ms; 0 old collections |
| Heap | 290 MB observed; 8.46 GB configured maximum |

Exact accounting passed: `1,069,201 + 10,800 = 1,080,001`. The raw payload-volume calculation is `1,080,001 × 102,400 bytes`; it excludes Kafka protocol framing and replication overhead. Final lag reached zero after the documented drain. Producer acknowledgement latency and drain duration include broker, batching, backpressure, and local-host effects; they are not core transform latency.

See the preserved [producer report](producer-results.json), [consumer report](consumer-results.json), [topic counts](final-topic-counts.txt), and [final lag](final-lag.txt).
