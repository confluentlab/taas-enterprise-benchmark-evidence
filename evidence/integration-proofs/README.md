# Integration proof matrix

These are local execution proofs, not vendor certifications. Third-party names identify tested technical execution paths only; they do not imply sponsorship, partnership, endorsement, or certification. Statuses follow the repository [evidence classification](../../docs/evidence-classification.md).

## Latest preserved evidence

| Path | Evidence record | Status | Superseded? |
|---|---|---|---|
| HTTP batch | This matrix’s 60,000/60,000 local batch record | `LIVE_LOCAL_VERIFIED` | No later public run is preserved. |
| HTTP single | [`http-single-60000`](../historical-attempts/http-single-60000.md) | `INCOMPLETE` | No equivalent full passing rerun is preserved. |
| gRPC contract | [`runtime-parity-20260712`](../runtime-parity/summary.md) | `CONTRACT_VERIFIED` | Current for the preserved contract fixture. |
| Live gRPC service | [`grpc-live-attempt`](../historical-attempts/grpc-live-service.md) | `PRESERVED_FAILURE` | No; contract verification is a different boundary. |

| Path | Status | Preserved scope / result |
|---|---|---|
| Kafka Connect + MongoDB | `LIVE_LOCAL_VERIFIED` | Focused local native proof |
| Kafka Connect + PostgreSQL | `LIVE_LOCAL_VERIFIED` | Focused local native proof |
| Flink | `LIVE_LOCAL_VERIFIED` | Focused local native proof plus 1 MiB live probe |
| Kafka Streams | `LIVE_LOCAL_VERIFIED` | Focused local native proof |
| Spring Boot | `LIVE_LOCAL_VERIFIED` | Focused local embedded proof |
| HTTP batch | `LIVE_LOCAL_VERIFIED` | 60,000 / 60,000; 649.34 records/s |
| HTTP single | `INCOMPLETE` | 59,600 / 60,000; 400 failed; 129.99 records/s |
| gRPC unary and stream | `PRESERVED_FAILURE` | Live service returned `UNIMPLEMENTED` |
| Kafka Connect S3 | `PRESERVED_FAILURE` | Connector creation returned HTTP 500 |
| WarpStream / Bento local | `LIVE_LOCAL_VERIFIED` | Small 50-record local proofs |
| Pulsar HTTP bridge | `LIVE_LOCAL_VERIFIED` | Small 50-record local proof |
| NiFi, Spark, Redpanda Connect, Logstash, Vector | `MEASURED` | HTTP/sidecar paths; not first-class native wrappers |
| Camel, Beam, Spring Cloud Stream, Debezium, OpenTelemetry | `MEASURED` | Local measurements; no formal qualification applied |

Additional high-value local proofs are summarized in [HTTP tools](http-tools.md). See [Kafka/native](kafka-native.md), [gRPC](grpc.md), and [other runtimes](pulsar-and-other-runtimes.md).
