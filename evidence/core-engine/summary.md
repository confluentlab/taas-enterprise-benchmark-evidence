# Core engine: 1 MiB controlled benchmark

## Result

**Status: `MEASURED_NOT_QUALIFIED`.**

The benchmark completed successfully and produced valid latency and allocation measurements. It did not meet the repository’s stricter cross-run repeatability qualification because three of twelve mean-stability checks exceeded their configured 5% tolerance.

The original evaluator emitted `PROVISIONAL_REJECTED`. The public status preserves that raw result in [qualification-results.json](qualification-results.json) while describing its meaning more precisely.

## Timed boundary

The timed boundary starts with raw bytes and includes full scan and parse, 976 compiled mapping fields, transformations, policies and bounded errors, then serialization to owned bytes. Compilation and corpus setup are excluded. The exact input was 1,049,487 bytes with 100 variants. Full scan means the parser/scanner consumes the complete input byte sequence; it does not mean every input field is materialized into the output model.

## Measurements

| Mode | Mean | p50 | p95 | p99 | p99.9 | Allocation |
|---|---:|---:|---:|---:|---:|---:|
| Success | 503.620 µs/op | 424.960 µs | 680.960 µs | 1,300.480 µs | 1,767.424 µs | 187,999.9 B/op |
| Bounded errors | 517.701 µs/op | 447.488 µs | 656.384 µs | 1,325.056 µs | 1,781.760 µs | 198,336.1 B/op |

## Repeatability qualification

Nine of twelve checks met their configured thresholds. The three misses were:

| Check | Limit | Observed | Difference |
|---|---:|---:|---:|
| Success workload, group B mean spread | ≤ 5.000% | 7.763% | 2.763 percentage points over |
| Bounded-error workload, group A mean spread | ≤ 5.000% | 5.007% | 0.007 percentage points over |
| Bounded-error workload, group B mean spread | ≤ 5.000% | 8.525% | 3.525 percentage points over |

These checks compare variation between repeated benchmark measurements. They do not represent transformation failures, output mismatches, crashes, operation failures, or failed correctness tests. The evaluator does not prove whether the extra variation came from workstation noise or a runtime/workload effect, so the result is measured but not publication-qualified.

The complete 12-check table and group definitions are in [Repeatability qualification](../../docs/repeatability-qualification.md).

## Additional validation

- 174 tests passed.
- Evidence hashes verified.
- Output validation passed.
- No benchmark operation failures were reported.
- The source and environment identities were recorded.

The host used Windows 11, Oracle JDK 21.0.9, HotSpot/G1, 16 logical cores, approximately 33.82 GB RAM, AC power, and the Balanced power plan. The source worktree was a dirty, authorized experiment, so this result is not a release certification.

See [results](results.csv), [percentiles](percentiles.json), [qualification data](qualification-results.json), [environment](environment.txt), [methodology](methodology.md), and [interpretation](../../docs/benchmark-interpretation.md).
