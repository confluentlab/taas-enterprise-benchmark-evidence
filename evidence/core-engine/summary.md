# Core engine: 1 MiB provisional benchmark

**Status: PROVISIONAL_REJECTED.** The run was not publication-eligible because 3 of 12 repeatability gates missed their thresholds: `success.B.meanSpread`, `errors.A.meanSpread`, and `errors.B.meanSpread`.

The timed boundary starts with raw bytes and includes full scan/parse, a compiled 976-field mapping, transformations, policies and bounded errors, then serialization to owned bytes. Compilation and corpus setup are excluded. The exact input was 1,049,487 bytes with 100 variants.

| Mode | Mean | p50 | p95 | p99 | p99.9 | Allocation |
|---|---:|---:|---:|---:|---:|---:|
| Success | 503.620 µs/op | 424.960 µs | 680.960 µs | 1,300.480 µs | 1,767.424 µs | 187,999.9 B/op |
| Bounded errors | 517.701 µs/op | 447.488 µs | 656.384 µs | 1,325.056 µs | 1,781.760 µs | 198,336.1 B/op |

The host used Windows 11, Oracle JDK 21.0.9, HotSpot/G1, 16 logical cores, approximately 33.82 GB RAM, AC power, and the Balanced power plan. Raw evidence hashes verified and 174 tests passed. The source worktree was dirty for the authorized experiment, another reason to avoid presenting the result as a release certification.

See [results](results.csv), [percentiles](percentiles.json), [environment](environment.txt), [methodology](methodology.md), and [interpretation](../../docs/benchmark-interpretation.md).
