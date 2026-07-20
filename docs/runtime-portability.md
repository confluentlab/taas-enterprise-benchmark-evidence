# Runtime portability

This matrix reflects source inspection and preserved evidence as of 2026-07-19. “Native” describes an implementation module, not a vendor certification.

| Runtime path | Implementation | Assignment | Replay/schema/failure reporting | Latest public evidence |
|---|---|---|---|---|
| Embedded Java / Spring Boot | Native embedded | Yes | Yes | Source/tests; focused local pass |
| Kafka Connect SMT | Native | Yes | Yes | Focused MongoDB and PostgreSQL local passes |
| Kafka Streams | Native | Yes | Yes | Focused local pass |
| Apache Flink | Native | Yes | Yes | Focused local pass; latest 1 MiB local Docker proof |
| Bento | Assigned adapter | Yes | Replay when enabled | Small local proof |
| AWS/Azure/GCP serverless wrappers | Assigned wrappers | Yes | Replay when enabled | Wrapper/source coverage; selected emulator proofs |
| HTTP single/batch | Stateless synchronous API | No | No | Batch pass; latest full single run incomplete |
| gRPC batch/stream | Contract and implementation modules | No | No | Contract parity passes; latest live service returned `UNIMPLEMENTED` |
| NiFi, Spark, Redpanda Connect, Logstash, Vector | HTTP/sidecar paths | Varies | Varies | Measured local paths; not first-class native wrappers |

The runtime contract includes stable identity, capabilities, assignment polling, heartbeat, deployment state, bounded operational metrics, canonical failure records, replay, and schema observations. See [Runtime parity](../evidence/runtime-parity/summary.md) for output identity and [Integration proofs](../evidence/integration-proofs/README.md) for test status.
