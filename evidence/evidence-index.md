# Evidence index

| Evidence set | Captured | Classification | Status | Primary artifacts |
|---|---|---|---|---|
| Core engine, 1 MiB | 2026-07-17 | Controlled JMH | Provisional rejected | [summary](core-engine/summary.md), [results](core-engine/results.csv), [percentiles](core-engine/percentiles.json) |
| Payload scaling, 1-64 MiB | 2026-07-18 | Controlled JMH | Measured | [summary](payload-scaling/summary.md), [results](payload-scaling/results.csv), [regression](payload-scaling/regression.json) |
| Kafka/Flink soak | 2026-07-11 | Local Docker live run | Accounting passed | [summary](kafka-soak/summary.md), [producer](kafka-soak/producer-results.json), [consumer](kafka-soak/consumer-results.json) |
| Live Flink, 1 MiB | 2026-07-18 | Local Docker live probe | Measured | [summary](live-flink-runtime/summary.md), [latency](live-flink-runtime/latency-results.json), [allocation](live-flink-runtime/allocation-results.json) |
| Runtime output parity | 2026-07-12 | In-process contract test | Passed fixed fixtures | [summary](runtime-parity/summary.md), [raw report](runtime-parity/parity-output-report.json) |
| Integration proofs | through 2026-07-18 | Mixed local live runs | Mixed | [matrix](integration-proofs/README.md) |

All timestamps, scopes, and statuses refer only to the preserved runs. See the [claims matrix](claims-matrix.csv), [methodology](../docs/benchmark-methodology.md), [interpretation guide](../docs/benchmark-interpretation.md), and [checksums](checksums.sha256).
