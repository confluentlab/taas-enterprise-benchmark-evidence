# Flowplane: governed transformations with verifiable runtime evidence

**Define a transformation once, govern it centrally, and execute the same versioned artifact across multiple streaming runtimes with deterministic behavior.**

Flowplane is a governed transformation control plane and portable Java execution engine for authoring, validating, approving, deploying, observing, and rolling back field-level stream transformations. Production payloads are designed to remain inside the separately deployed data-plane runtime boundary. The control plane distributes versioned artifacts and receives bounded operational telemetry; runtime ownership and hosting depend on the deployment.

> **Evidence policy:** Every result in this repository is classified by execution boundary and verification level: controlled benchmark, live local integration, contract test, incomplete run, or preserved failure. Each claim links to its methodology, environment, and raw evidence.

[30-second demo](#30-second-demo) · [Performance](#benchmark-results) · [Runtime evidence](#runtime-evidence-matrix) · [Verification](#evidence-and-verification) · [Limitations](#scope-and-limitations)

| Evidence identity | Immutable value |
|---|---|
| Evidence release | `evidence-2026.07.2` |
| Flowplane source revision | `10a26df` ([full identity](evidence/manifest.json)) |
| Evidence repository snapshot | The commit referenced by `evidence-2026.07.2` |
| Evidence dates | 2026-07-11 through 2026-07-21 UTC; core benchmark tested 2026-07-17 |

Release `evidence-2026.07.2` includes the 2026-07-20/21 local-integration supplement: 22 streaming/tool integrations, eight runtime/protocol baselines, five core-surface stability soaks, and three provider-trigger proofs. See the [complete evidence overview](evidence/integration-proofs/EVIDENCE-OVERVIEW.md).

## 30-second demo

[![Flowplane evidence-safe product demo](assets/demo-poster.png)](assets/demo.mp4)

The demo shows how Flowplane authors, governs, and executes a mapping. The evidence graphic below reports what the preserved fixtures measured; it is not a second product walkthrough.

| Streaming soak | Runtime parity | Payload scaling | Core engine |
|---|---|---|---|
| **1,080,001 records** with exact accounting and final lag 0 | **5 contract execution modes** produced an identical output hash | **1–64 MiB** measured with R² 0.9989 | **1 MiB / 976 compiled mapping fields** measured at 0.504 ms mean |
| **Live local verified** (`LIVE_LOCAL_VERIFIED`) | **Contract verified** (`CONTRACT_VERIFIED`) | **Measured** (`MEASURED`) | **Measured, not qualified** (`MEASURED_NOT_QUALIFIED`) |

All public benchmark and integration fixtures use synthetic data. Results apply only to the documented workload, boundary, and environment.

## What Flowplane solves

Transformation logic is often duplicated across connectors, stream processors, sidecars, and serverless handlers. That makes policy review, rollout, rollback, runtime parity, and audit evidence difficult. Flowplane separates governed mapping management from execution while preserving an explicit contract between them.

## Why this matters

Flowplane is designed to reduce the cost and risk of maintaining transformation logic independently across streaming systems. The evidence tests three properties central to that design:

- Whether complex transformations remain computationally practical.
- Whether the same artifact behaves deterministically across execution boundaries.
- Whether runtime processing can complete a sustained streaming workload with reconciled outputs and failures.

## What has been demonstrated

### Core-engine performance

The controlled benchmark completed successfully at 0.504 ms mean, 1.300 ms p99, and approximately 188 KB allocated per operation for the documented 1 MiB workload with 976 compiled mapping fields. Nine of twelve repeatability checks met their configured thresholds. Three cross-run mean-stability checks exceeded the 5% tolerance: one success-path comparison and two bounded-error comparisons.

These misses concern run-to-run variance. They do not indicate incorrect transformation output, operation failures, crashes, or failed correctness tests. [Understand the repeatability qualification](docs/repeatability-qualification.md).

### Payload-size scaling

The frozen workload with 976 compiled mapping fields measured 0.473 ms at 1 MiB and 19.387 ms at 64 MiB. Its linear fit was R² 0.998895. The added bulk field was scanned but not referenced, and normalized output remained 36,478 bytes.

### Streaming soak and accounting

The 30-minute local Kafka/Flink run produced 1,080,001 synthetic 102,400-byte records, or 110,591,022,399 raw payload bytes (approximately 103.0 GiB before Kafka framing and replication). It achieved 599.876 produced records/s against a configured 600 records/s target, completed at 532.145 records/s, emitted 1,069,201 successful outputs and 10,800 intentional failures, and reached final lag zero after a 2,029.525-second drain.

### Cross-runtime deterministic behavior

A fixed mapping and fixture produced the same valid-output SHA-256 through embedded Java, HTTP single, HTTP batch, gRPC batch, and gRPC stream contract modes. This is contract verification, not a live gRPC deployment claim.

Separate 2026-07-21 live-local runs subsequently exercised current HTTP single/batch and gRPC batch/bidirectional-streaming sidecars. Each completed a 110-record baseline, and all five core JVM/HTTP/gRPC surfaces completed independent 10,000-record/five-minute soaks with exact accounting, zero final lag, and healthy periodic observations. These newer runs prove the documented local sidecar boundary; they do not turn the earlier contract fixture into a deployment benchmark.

![Latest evidence summary](assets/benchmark-summary.svg)

## Architecture

1. Authors create versioned mappings with validation, transforms, and policy rules.
2. Simulation shows before/after payloads, field failures, policy results, and latency.
3. Approval and deployment controls produce an immutable artifact.
4. A separately deployed data-plane runtime verifies and executes the artifact locally, then reports bounded telemetry and canonical failures.
5. Operators inspect drift, failures, and rollout state, then promote or roll back.

![Flowplane architecture](assets/architecture.svg)

See [How it works](docs/how-it-works.md) and [Architecture](docs/architecture.md).

## Runtime evidence matrix

### Native runtimes

| Runtime | Execution path | Evidence status |
|---|---|---|
| Embedded Java / Spring | In-process native engine | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Kafka Connect SMT | Native connector transform | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Kafka Streams | Native topology integration | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Apache Flink | Native map/runtime integration | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |

### Flowplane protocols

| Protocol | Execution path | Evidence status |
|---|---|---|
| HTTP single | Stateless synchronous API | **Live local verified** (`LIVE_LOCAL_VERIFIED`), including a 10,000-record/five-minute soak; older 60,000-record attempt remains `INCOMPLETE` |
| HTTP batch | Stateless batch API | **Live local verified** (`LIVE_LOCAL_VERIFIED`), including a 10,000-record/five-minute soak |
| gRPC batch | Live sidecar `TransformBatch` | **Live local verified** (`LIVE_LOCAL_VERIFIED`), including a 10,000-record/five-minute soak; older `UNIMPLEMENTED` attempt remains historical |
| gRPC streaming | Live sidecar `TransformStream` | **Live local verified** (`LIVE_LOCAL_VERIFIED`), including a 10,000-record/five-minute soak; older `UNIMPLEMENTED` attempt remains historical |

### Tool interoperability

| Tools | Execution path | Evidence status |
|---|---|---|
| Pulsar, ActiveMQ Classic/Artemis, NATS JetStream, Redis Streams, RabbitMQ Streams, EMQX/MQTT, RocketMQ | Broker-native input/output plus HTTP/sidecar execution | **Live local verified** (`LIVE_LOCAL_VERIFIED`) for focused 100-valid/10-invalid fixtures |
| Redpanda Connect, Logstash, Camel, Spring Cloud Stream, NiFi, Spark, Beam, Bento, Vector, OpenTelemetry Collector, Debezium | Tool/framework integration, primarily HTTP or sidecar | **Live local verified** (`LIVE_LOCAL_VERIFIED`) for focused 100-valid/10-invalid fixtures |
| AWS, Azure, and GCP HTTP wrappers | Assigned serverless wrappers | **Live local verified** (`LIVE_LOCAL_VERIFIED`) in local Docker images |
| Azure Queue/Event Hub and GCP Pub/Sub CloudEvent triggers | Provider handler plus local emulator | Local trigger contract **passed**; GCP includes a documented envelope-only Eventarc substitute |

Third-party names identify tested technical execution paths only. They do not imply sponsorship, partnership, endorsement, or certification. See the full [runtime portability matrix](docs/runtime-portability.md), [proven integration overview](evidence/integration-proofs/EVIDENCE-OVERVIEW.md), and [historical attempts](evidence/historical-attempts/README.md).

### Latest evidence by protocol

| Path | Latest preserved evidence | Current status | Superseded? |
|---|---|---|---|
| HTTP batch | [10,000-record/five-minute run](evidence/integration-proofs/http-batch/runs/20260721T054136Z/summary.md) | **Live local verified** (`LIVE_LOCAL_VERIFIED`) | Latest local supplement run. |
| HTTP single | [10,000-record/five-minute run](evidence/integration-proofs/http-single/runs/20260721T054134Z/summary.md) | **Live local verified** (`LIVE_LOCAL_VERIFIED`) | Supersedes the older incomplete run as current interoperability evidence, but does not make that 60,000-record workload pass. |
| gRPC contract | [`runtime-parity-20260712`](evidence/runtime-parity/summary.md) | **Contract verified** (`CONTRACT_VERIFIED`) | Current for the preserved fixture and contract boundary. |
| Live gRPC batch | [10,000-record/five-minute run](evidence/integration-proofs/grpc-batch/runs/20260721T054138Z/summary.md) | **Live local verified** (`LIVE_LOCAL_VERIFIED`) | Newer sidecar build; older `UNIMPLEMENTED` run remains historical. |
| Live gRPC streaming | [10,000-record/five-minute run](evidence/integration-proofs/grpc-streaming/runs/20260721T054141Z/summary.md) | **Live local verified** (`LIVE_LOCAL_VERIFIED`) | Newer sidecar build; older `UNIMPLEMENTED` run remains historical. |

## Benchmark results

### Core engine: 1 MiB workload complexity

```text
Input:                   1,049,487 bytes
Input variants:          100
Compiled mapping fields: 976
Execution:               full scan/parse/transform/policies/errors/serialize
Success mean:            503.620 µs
Success p99:             1,300.480 µs
Allocation:              187,999.9 B/op
Associated semantic suite: 174 tests passed
Repeatability:           9/12 checks met
```

Here, **full scan** means the parser/scanner consumes the complete input byte sequence. It does not mean every input field is materialized into the output model.

| Evidence | Workload | Result | Qualification |
|---|---|---:|---|
| Core engine, 1 MiB | 976 compiled mapping fields; full scan, parse, transform, policy handling, and serialization | 0.504 ms mean; 1.300 ms p99; ~188 KB/op | **Measured, not qualified** (`MEASURED_NOT_QUALIFIED`): benchmark completed; three cross-run mean-stability checks exceeded tolerance |
| Payload scaling | 1–64 MiB; same 976 compiled mapping fields | 0.473–19.387 ms mean; linear fit R² 0.9989 | **Measured** (`MEASURED`) |
| Kafka/Flink local soak | 30 min; synthetic 102,400-byte records; configured target 600 records/s | 1,080,001 produced at 599.876 records/s; 103.0 GiB raw payload; 532.145 completed records/s; 2,029.525 s drain; final lag 0 | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Live Flink transform | 1 MiB; 1,001 outputs | p50 2.512 ms; p95 11.438 ms; p99 17.294 ms; DLQ 0 | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Runtime output parity | Java, HTTP single/batch, gRPC batch/stream contract modes | Identical valid-output SHA-256 | **Contract verified** (`CONTRACT_VERIFIED`) |

Allocation depends on whether large fields are referenced and materialized. The scaling fixture intentionally held output size constant; see the [interpretation guide](docs/benchmark-interpretation.md) for a referenced 16 MiB-field measurement.

### Measured boundaries

| Evidence | Input boundary | Output boundary | Included work | Excluded work | Environment | Status |
|---|---|---|---|---|---|---|
| Core-engine JMH | Raw input bytes | Owned serialized output bytes | Scan, parse, mapping, transforms, policies, bounded errors, serialization | Compilation, corpus generation, network, broker, runtime wrapper | Windows 11; Oracle JDK 21.0.9; HotSpot/G1 | **Measured, not qualified** (`MEASURED_NOT_QUALIFIED`) |
| Live Flink runtime | Kafka input record | Runtime output/DLQ record | Kafka consumption, runtime execution, serialization, relevant runtime effects | Producer creation and unrelated control-plane work | Local Docker; four Kafka partitions | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |
| Kafka/Flink soak | Producer submission | Final output/error accounting and lag | Producer acknowledgements, broker behavior, consumer/runtime processing, drain | Isolated engine attribution | Local Docker; 30-minute sustained run | **Live local verified** (`LIVE_LOCAL_VERIFIED`) |

Charts: [mean scaling](evidence/payload-scaling/charts/mean-latency.svg), [representative tail latency](evidence/payload-scaling/charts/tail-latency.svg), [allocation scaling](evidence/payload-scaling/charts/allocation.svg), and [soak accounting/lag](evidence/kafka-soak/charts/accounting-and-lag.svg).

## Governance

- Versioned mapping management
- Segregated approval and deployment controls
- Immutable artifacts and integrity verification
- Tenant- and workload-aware access controls
- Staged deployment and rollback
- Auditability and bounded telemetry
- Payload-retention minimization

Detailed implementation controls remain in [Governance and security](docs/governance-and-security.md).

## Evidence and verification

- [Evidence classification](docs/evidence-classification.md)
- [Evidence manifest](evidence/manifest.json)
- [Evidence index](evidence/evidence-index.md)
- [Proven local integration evidence](evidence/integration-proofs/EVIDENCE-OVERVIEW.md)
- [Integration reproduction guide](reproduction/README.md)
- [Claims matrix](evidence/claims-matrix.csv)
- [Core benchmark qualification](docs/repeatability-qualification.md)
- [Runtime parity matrix](evidence/runtime-parity/parity-matrix.csv)
- [Checksums](evidence/checksums.sha256)
- Release: [`evidence-2026.07.1`](https://github.com/Flowplane/flowplane-evidence/releases/tag/evidence-2026.07.1)

```bash
python scripts/validate-live-local-evidence.py
python scripts/validate-evidence.py
sh scripts/verify-checksums.sh
```

## Public evidence boundary

This repository intentionally excludes Flowplane’s production source code, compiler and parser implementation, complete transformation grammar, optimization strategy, persistence model, production infrastructure, authentication material, and proprietary mapping artifacts. Published fixtures are synthetic and simplified.

## Scope and limitations

Results apply only to their documented fixtures, boundaries, and environments. Core JMH and live runtime measurements are not interchangeable. Local Docker and emulator results are not managed-cloud certification. Contract verification is separate from live deployment proof. The five-minute soak claim applies only to the five named JVM/HTTP/gRPC surfaces. No universal throughput, runtime-equivalence, security-audit, or vendor-certification claim is made. See [Scope and limitations](docs/limitations.md).

## Contact

Open a GitHub issue for evidence questions. For security vulnerabilities, follow [SECURITY.md](SECURITY.md).
