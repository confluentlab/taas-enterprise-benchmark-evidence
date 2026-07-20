# Integration proof matrix

These are local execution proofs, not vendor certifications. `PASS` means the preserved scope completed; `MEASURED` means useful measurements exist but the full pass criteria were not met or were not applicable; `FAIL` preserves a failed gate.

| Path | Status | Preserved scope / result |
|---|---|---|
| Kafka Connect + MongoDB | PASS | Focused local native proof |
| Kafka Connect + PostgreSQL | PASS | Focused local native proof |
| Flink | PASS | Focused local native proof plus 1 MiB live probe |
| Kafka Streams | PASS | Focused local native proof |
| Spring Boot | PASS | Focused local embedded proof |
| HTTP batch | PASS | 60,000 / 60,000; 649.34 records/s |
| HTTP single | FAIL | 59,600 / 60,000; 400 failed; 129.99 records/s |
| gRPC unary and stream | FAIL | Live service returned `UNIMPLEMENTED` |
| Kafka Connect S3 | FAIL | Connector creation returned HTTP 500 |
| WarpStream / Bento local | PASS | Small 50-record local proofs |
| Pulsar HTTP bridge | PASS | Small 50-record local proof |
| NiFi, Spark, Redpanda Connect, Logstash, Vector | MEASURED | HTTP/sidecar paths; not first-class native wrappers |
| Camel, Beam, Spring Cloud Stream, Debezium, OpenTelemetry | MEASURED | Local measurements; full matrix criteria not passed |

Additional high-value local proofs are summarized in [HTTP tools](http-tools.md). See [Kafka/native](kafka-native.md), [gRPC](grpc.md), and [other runtimes](pulsar-and-other-runtimes.md).
