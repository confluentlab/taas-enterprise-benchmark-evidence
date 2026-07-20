# Core benchmark repeatability qualification

The 1 MiB controlled benchmark completed and produced valid latency and allocation measurements. Its status is `MEASURED_NOT_QUALIFIED` because 9 of 12 cross-run stability checks met their thresholds. Three mean-latency spread checks exceeded the configured 5% tolerance.

## Why repeated groups exist

One benchmark launch can look unusually fast or slow because of host scheduling, CPU frequency, thermal state, garbage collection, or other workstation noise. The publication protocol uses four groups of three independent launches, with 30-second cooldowns, to test whether the reported result remains stable across fresh JVM launches.

- **Group A** supplies the clean average-latency result.
- **Group B** supplies percentile results and an independent mean-latency comparison.
- **Group C** supplies allocation results and profiler-supported diagnostics.
- **Group D** enables sample-level diagnostic profiling. Its instrumentation can change latency, so the results are retained for analysis but excluded from publication qualification.

“Mean spread” is the relative range between repeated mean-latency measurements inside a group. A value of 0.05 means 5%. “A-vs-B mean difference” compares the two independent group aggregates. Percentile and allocation spreads use the same relative form for their named metric.

## Individual launch means

The evaluator calculates each within-group spread as `(maximum launch mean - minimum launch mean) / median launch mean`. Values below are the preserved per-launch JMH means, rounded to three decimals for display; the evaluator uses full-precision values from the raw files.

| Workload / group | Launch 1 (µs/op) | Launch 2 (µs/op) | Launch 3 (µs/op) | Median (µs/op) | Spread |
|---|---:|---:|---:|---:|---:|
| Success A | 497.258 | 510.725 | 503.620 | 503.620 | 2.674% |
| Success B | 460.032 | 467.262 | 496.306 | 467.262 | 7.763% |
| Bounded errors A | 517.701 | 527.598 | 501.676 | 517.701 | 5.007% |
| Bounded errors B | 484.274 | 475.799 | 517.082 | 484.274 | 8.525% |

Groups C and D also contain three launches. Group C provides allocation qualification and profiler-supported diagnostics; group D provides sample-profiler diagnostics only. Group D’s measured profiler-effect ratios differ from clean timing, which is why it is not used to accept or reject the latency publication gates.

## All 12 checks

| Gate | Human-readable meaning | Limit | Observed | Result | Interpretation |
|---|---|---:|---:|---|---|
| `success.A.meanSpread` | Success mean-latency spread across group A launches | ≤ 5.000% | 2.674% | Met | Clean success means were within tolerance. |
| `success.B.meanSpread` | Success mean-latency spread across group B launches | ≤ 5.000% | 7.763% | Missed by 2.763 pp | Group B success means varied beyond tolerance. |
| `success.A-vs-B.meanDifference` | Difference between success mean aggregates from groups A and B | ≤ 10.000% | 7.219% | Met | Independent success aggregates agreed within tolerance. |
| `success.C.allocationSpread` | Success allocation spread across group C launches | ≤ 2.000% | 0.248% | Met | Success allocation was stable. |
| `success.B.p99Spread` | Success p99 spread across group B launches | ≤ 15.000% | 9.134% | Met | Success p99 was within its tail-latency tolerance. |
| `success.B.p99.9Spread` | Success p99.9 spread across group B launches | ≤ 20.000% | 4.592% | Met | Success p99.9 was within tolerance. |
| `errors.A.meanSpread` | Bounded-error mean-latency spread across group A launches | ≤ 5.000% | 5.007% | Missed by 0.007 pp | Group A error means were just outside tolerance. |
| `errors.B.meanSpread` | Bounded-error mean-latency spread across group B launches | ≤ 5.000% | 8.525% | Missed by 3.525 pp | Group B error means varied beyond tolerance. |
| `errors.A-vs-B.meanDifference` | Difference between bounded-error mean aggregates from groups A and B | ≤ 10.000% | 6.457% | Met | Independent error aggregates agreed within tolerance. |
| `errors.C.allocationSpread` | Bounded-error allocation spread across group C launches | ≤ 2.000% | 0.086% | Met | Error-path allocation was stable. |
| `errors.B.p99Spread` | Bounded-error p99 spread across group B launches | ≤ 15.000% | 8.037% | Met | Error-path p99 was within tolerance. |
| `errors.B.p99.9Spread` | Bounded-error p99.9 spread across group B launches | ≤ 20.000% | 8.069% | Met | Error-path p99.9 was within tolerance. |

| Category | Met | Total |
|---|---:|---:|
| Success-path mean stability | 2 | 3 |
| Success-path percentile stability | 2 | 2 |
| Bounded-error mean stability | 1 | 3 |
| Bounded-error percentile stability | 2 | 2 |
| Allocation stability | 2 | 2 |
| **Overall** | **9** | **12** |

## What did not fail

These gate misses do not indicate transformation failures, incorrect output, crashes, operation failures, or allocation growth. The benchmark completed, raw evidence hashes verified, output validation passed, and the associated pre-publication semantic suite passed 174 tests. The qualification result means that three cross-run mean-latency comparisons varied beyond the repository’s configured repeatability tolerance.

The evaluator does not identify a single cause. The observed variance may include workstation noise or an unresolved workload/runtime effect. Without a qualified baseline comparison, these misses should not be labeled a performance regression.

## How to use the measured result

The 503.620 µs mean and 1,300.480 µs p99 remain usable for engineering analysis of the documented workload and host. They are not a universal latency guarantee and should not be promoted as a publication-qualified benchmark.

A future run becomes `QUALIFIED` only when all 12 checks meet their limits under the controlled protocol, evidence hashes and semantic tests pass, output validation passes, and the full environment and source identity are preserved.

Machine-readable values are in [qualification-results.json](../evidence/core-engine/qualification-results.json). See the [benchmark methodology](benchmark-methodology.md) and [interpretation guide](benchmark-interpretation.md).
