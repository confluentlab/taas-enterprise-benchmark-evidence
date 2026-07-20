# Evidence index

| Evidence set | Captured | Classification | Status | Primary artifacts |
|---|---|---|---|---|
| Core engine, 1 MiB | 2026-07-17 | Controlled JMH | `MEASURED_NOT_QUALIFIED` | [summary](core-engine/summary.md), [results](core-engine/results.csv), [qualification](core-engine/qualification-results.json) |
| Payload scaling, 1–64 MiB | 2026-07-18 | Controlled JMH | `MEASURED` | [summary](payload-scaling/summary.md), [results](payload-scaling/results.csv), [regression](payload-scaling/regression.json) |
| Kafka/Flink soak | 2026-07-11 | Local Docker live run | `LIVE_LOCAL_VERIFIED` | [summary](kafka-soak/summary.md), [producer](kafka-soak/producer-results.json), [consumer](kafka-soak/consumer-results.json) |
| Live Flink, 1 MiB | 2026-07-18 | Local Docker live probe | `LIVE_LOCAL_VERIFIED` | [summary](live-flink-runtime/summary.md), [latency](live-flink-runtime/latency-results.json), [allocation](live-flink-runtime/allocation-results.json) |
| Runtime output parity | 2026-07-12 | In-process contract test | `CONTRACT_VERIFIED` | [summary](runtime-parity/summary.md), [matrix](runtime-parity/parity-matrix.csv), [raw report](runtime-parity/parity-output-report.json) |
| Integration proofs | through 2026-07-18 | Mixed local live runs | Multiple classified statuses | [matrix](integration-proofs/README.md), [historical attempts](historical-attempts/README.md) |

All timestamps, scopes, and statuses refer only to the preserved runs. See the [claims matrix](claims-matrix.csv), [methodology](../docs/benchmark-methodology.md), [interpretation guide](../docs/benchmark-interpretation.md), and [checksums](checksums.sha256).

The machine-readable [manifest](manifest.json) binds run IDs, approved statuses, summaries, raw results, environments, methodologies, charts, and primary-result hashes. Incomplete and failed runs remain available under [historical attempts](historical-attempts/README.md).
