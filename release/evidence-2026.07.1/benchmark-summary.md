# Flowplane evidence release 2026.07.1

This release fixes the public evidence vocabulary and preserves the current benchmark, soak, runtime, and parity results under one immutable tag.

| Evidence | Status | Result |
|---|---|---|
| Core engine, 1 MiB | `MEASURED_NOT_QUALIFIED` | 503.620 µs mean; 1,300.480 µs p99; 187,999.9 B/op; 9/12 repeatability checks met |
| Payload scaling, 1–64 MiB | `MEASURED` | 0.473–19.387 ms mean; R² 0.998895 for the fixed-output fixture |
| Kafka/Flink soak | `LIVE_LOCAL_VERIFIED` | 1,080,001 inputs; exact success/error accounting; final lag zero |
| Live Flink, 1 MiB | `LIVE_LOCAL_VERIFIED` | 1,001 outputs; p50 2.512 ms; p95 11.438 ms; zero DLQ |
| Runtime parity | `CONTRACT_VERIFIED` | Identical expected output and canonical error across five contract modes |

The core benchmark completed correctly but did not meet the cross-run repeatability qualification. Three mean-latency spread checks exceeded the 5% limit. See [Repeatability qualification](../../docs/repeatability-qualification.md).

Release assets are derived from [manifest.json](../../evidence/manifest.json) and [checksums.sha256](../../evidence/checksums.sha256). Results are not universal throughput guarantees or vendor certifications.
