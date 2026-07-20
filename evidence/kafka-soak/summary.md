# Kafka/Flink 30-minute soak

**Status: `LIVE_LOCAL_VERIFIED`.** The live local run met its completion and exact-accounting criteria.

The local Docker soak targeted 600 records/s for 1,800 seconds using 102,400-byte payloads and a deliberate 1% missing-tenant failure path.

| Measure | Result |
|---|---:|
| Produced | 1,080,001 records |
| Producer failures | 0 |
| Produced bytes | 110,591,022,399 |
| Actual producer rate | 599.876 records/s; 58.581 MiB/s |
| Successful outputs | 1,069,201 |
| Intentional failures | 10,800 |
| Final raw-topic lag | 0 |
| Transform latency | p50 779 µs; p95 7,124 µs; p99 15,596 µs; p99.9 32,031 µs |
| End-to-end streaming rate | 532.145 records/s; 51.967 MiB/s |
| GC | 1,491 young collections / 2,700 ms; 0 old collections |
| Heap | 290 MB observed; 8.46 GB configured maximum |

Exact accounting passed: `1,069,201 + 10,800 = 1,080,001`. Final lag reached zero after a 2,029.525-second drain. Producer acknowledgement latency and drain duration include broker, batching, backpressure, and local-host effects; they are not core transform latency.

See the preserved [producer report](producer-results.json), [consumer report](consumer-results.json), [topic counts](final-topic-counts.txt), and [final lag](final-lag.txt).
