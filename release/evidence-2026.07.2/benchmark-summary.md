# Flowplane evidence release 2026.07.2

This release incorporates the 2026-07-20/21 live-local supplement and hardens its public verification boundary.

| Evidence | Status | Result |
|---|---|---|
| Streaming and ecosystem integrations | Mixed: 18 `LIVE_LOCAL_VERIFIED`, 4 `MEASURED_NOT_QUALIFIED` | 22 focused local proofs; all retain exact output/error accounting, while Beam, Flink, Kafka Connect, and Spark lack broker-derived final-lag rows |
| Runtime and protocol baselines | `LIVE_LOCAL_VERIFIED` | Embedded Spring, HTTP single/batch, gRPC batch/streaming, and AWS/Azure/GCP HTTP wrappers |
| Five-minute core-surface soaks | `LIVE_LOCAL_VERIFIED` | 50,000 aggregate inputs; each of five surfaces ran at least five minutes with exact accounting |
| Provider triggers | `LIVE_LOCAL_VERIFIED — emulator` | Azure Queue, Azure Event Hub, and GCP Pub/Sub CloudEvent; 300 outputs and 30 intentional DLQ records |
| Earlier benchmark, soak, Flink, and parity evidence | Mixed documented statuses | Preserved unchanged from `evidence-2026.07.1` |

Across the 38 supplement bundles, the preserved accounting is 53,630 attempted inputs, 52,800 transformed outputs, and 830 intentional DLQ outputs. The central validator independently recomputes canonical output multisets, fixture-owned error-contract matches, observable record-ID duplication, loaded artifact identity, accounting, Kafka lag, and recorded write-boundary counters from preserved files.

All 35 integration/runtime bundles were executed from dirty, identified Flowplane worktrees. These are authorized local interoperability and stability proofs, not clean-tree release certifications. Local Docker and emulator results are not vendor certification or managed-cloud parity.

Expected transformed records come from Flowplane control-plane simulation for the same artifact and fixtures. Simulation is the expected baseline, not an independent implementation oracle. Eight preserved HTTP/gRPC protocol bundles contain 440 DLQ projections without record IDs; their counts and error contracts are verified, but they are excluded from the DLQ-ID duplicate claim. Protocol DLQ projections normalize error fields and are not byte-for-byte unchanged sidecar responses. Mounted bridge/JAR identity comparison is enforced for future captures and is not claimed retroactively for these bundles.

See the [evidence overview](../../evidence/integration-proofs/EVIDENCE-OVERVIEW.md), [classification vocabulary](../../docs/evidence-classification.md), and [reproduction guide](../../reproduction/live-local-verification/README.md).
