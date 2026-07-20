# Changelog

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
