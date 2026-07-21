# Historical attempts

This directory preserves incomplete and failed live attempts separately from current measured and verified evidence. Each record states the objective, completion criterion, observed outcome, known cause, remediation status, and whether later evidence supersedes it. The [latest preserved evidence table](../integration-proofs/README.md#latest-preserved-evidence) distinguishes the current contract and live status for HTTP and gRPC paths.

| Attempt | Status | Outcome | Superseded? |
|---|---|---|---|
| [HTTP single: 60,000 records](http-single-60000.md) | `INCOMPLETE` | 59,600 completed; 400 failed | Newer 110-record and 10,000-record local runs passed, but they do not make this distinct 60,000-record workload pass |
| [gRPC live service](grpc-live-service.md) | `PRESERVED_FAILURE` | Unary and stream returned `UNIMPLEMENTED` | Newer sidecar builds passed live batch and bidirectional-streaming baselines and 10,000-record soaks |
| [Kafka Connect S3](kafka-connect-s3.md) | `PRESERVED_FAILURE` | Connector creation returned HTTP 500 | No later passing S3 run preserved |

See [Evidence classification](../../docs/evidence-classification.md). These records are engineering history, not current capability certification.
