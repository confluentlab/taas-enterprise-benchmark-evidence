# Runtime portability

This matrix reflects source inspection and preserved evidence as of 2026-07-19. “Native” describes an implementation module, not a vendor certification.

| Runtime path | Implementation | Assignment | Replay/schema/failure reporting | Latest public evidence |
|---|---|---|---|---|
| Embedded Java / Spring Boot | Native embedded | Yes | Yes | `LIVE_LOCAL_VERIFIED` |
| Kafka Connect SMT | Native | Yes | Yes | `LIVE_LOCAL_VERIFIED` for focused MongoDB and PostgreSQL fixtures |
| Kafka Streams | Native | Yes | Yes | `LIVE_LOCAL_VERIFIED` |
| Apache Flink | Native | Yes | Yes | `LIVE_LOCAL_VERIFIED`, including the 1 MiB local Docker proof |
| Bento | Assigned adapter | Yes | Replay when enabled | `LIVE_LOCAL_VERIFIED` for a small local fixture |
| AWS/Azure/GCP serverless wrappers | Assigned wrappers | Yes | Replay when enabled | `NOT_TESTED` as a single cross-cloud qualification suite |
| HTTP single/batch | Stateless synchronous API | No | No | Batch `LIVE_LOCAL_VERIFIED`; latest full single run `INCOMPLETE` |
| gRPC batch/stream | Contract and implementation modules | No | No | `CONTRACT_VERIFIED`; live service attempt `PRESERVED_FAILURE` |
| NiFi, Spark, Redpanda Connect, Logstash, Vector | HTTP/sidecar paths | Varies | Varies | `MEASURED`; not first-class native wrappers |

The runtime contract includes stable identity, capabilities, assignment polling, heartbeat, deployment state, bounded operational metrics, canonical failure records, replay, and schema observations. See [Runtime parity](../evidence/runtime-parity/summary.md) for output identity and [Integration proofs](../evidence/integration-proofs/README.md) for test status.
