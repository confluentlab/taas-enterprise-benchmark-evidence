# Changelog

## 2026-07-21

- Prepared immutable release `evidence-2026.07.2` for the complete 2026-07-20/21 live-local supplement.
- Removed contradictory `not-executed.json` placeholders from all 35 successful integration/runtime bundles and made their presence a validation failure.
- Made the central validator recompute output multisets, fixture-owned error contracts, observable record-ID duplication, loaded artifact identity, accounting, Kafka lag, and recorded downstream-write counters from preserved evidence.
- Corrected the gRPC DLQ projection, Beam transport-error attribution, dirty-worktree, simulation-baseline, deployment-ownership, private-source, provider-emulator, and historical gRPC wording.
- Hardened future captures to reject count-derived Kafka lag, detect duplicates by record ID, preserve record IDs in protocol DLQ projections, and compare mounted bridge/JAR hashes with their host artifacts.
- Added 38 checksum-verified local evidence bundles covering 22 streaming/tool integrations, eight runtime/protocol baselines, five core-surface stability soaks, and three provider-trigger proofs.
- Corrected Beam, Flink, Kafka Connect, and Spark to `MEASURED_NOT_QUALIFIED`: their content accounting passed, but preserved Kafka inspection did not contain broker-derived lag rows.
- Preserved 53,630 attempted inputs, 52,800 transformed outputs, and 830 intentional DLQ outputs with no duplicate transformed IDs, no duplicate observable DLQ IDs, and no unexplained loss. Historical HTTP/gRPC DLQ projections without record IDs are excluded from the DLQ-ID claim; mounted bridge/JAR comparison applies to future captures until those surfaces are rerun.
- Added 62 run-bundle screenshots, four supplemental control-plane screenshots, complete manifests, outputs, logs, metrics, write-boundary audits, and reproduction commands.
- Added the exact local verification harness, canonical fixture, evidence import pipeline, and a validator for checksums, accounting, stability health, trigger content, and raw-only producer boundaries.
- Updated HTTP and gRPC status wording to distinguish current successful sidecar runs from preserved historical incomplete and `UNIMPLEMENTED` attempts.

## 2026-07-20

- Reframed the 1 MiB result as `MEASURED_NOT_QUALIFIED` while preserving the evaluator’s original status.
- Published all 12 repeatability thresholds and observations: 9 met, 3 mean-spread checks missed.
- Clarified that correctness, output validation, operation completion, percentile stability, and allocation stability passed.
- Added the approved evidence-status vocabulary, evidence manifest, measured-boundary matrix, expanded parity matrix, historical attempts, and public evidence boundary.
- Added four evidence-derived charts with workload, environment, date, status, raw path, and non-guarantee labels.
- Expanded CI checks for claims, paths, statuses, checksums, qualification, accounting, parity, charts, and release artifacts.

## 2026-07-19

- Rebuilt the public evidence repository from current FlowPlane code and preserved evidence.
- Added the 1,080,001-record Kafka/Flink soak with exact accounting and final lag.
- Added measured 1 MiB core-engine results with repeatability thresholds and observations disclosed.
- Added 1-64 MiB payload-scaling results and the referenced-output allocation caveat.
- Added the latest 1 MiB live Flink latency and allocation proof.
- Added runtime contract parity hashes and a source-support versus live-proof matrix.
- Added checksums, validation scripts, public-safe examples, and an evidence-safe demo.
