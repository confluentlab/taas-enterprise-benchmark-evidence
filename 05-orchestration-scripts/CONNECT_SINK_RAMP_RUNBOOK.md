# Connect Sink Ramp Orchestration

Standard harness:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa\run-connect-sink-ramp-suite.ps1 `
  -SuiteName flowplane-connect-byteconverter-25rps-2min `
  -Rates 25 `
  -DurationSecondsPerRate 120 `
  -Connectors mongo,postgres,s3 `
  -Mode isolated
```

The harness creates a fresh suite under:

```text
bench-runs\multi-runtime-1mb\<SuiteName>
```

Each connector run gets:

- fresh raw topic
- fresh DLQ topic
- fresh sink target
- connector config snapshot
- producer log
- lightweight 10-second monitor CSV
- connector status
- compact `run-end.json`
- optional FLOWPLANE transform probe summary

## Common Runs

Small ramp:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa\run-connect-sink-ramp-suite.ps1 `
  -SuiteName flowplane-connect-byteconverter-25rps-2min `
  -Rates 25 `
  -DurationSecondsPerRate 120
```

Longer single rate:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa\run-connect-sink-ramp-suite.ps1 `
  -SuiteName flowplane-connect-byteconverter-25rps-5min `
  -Rates 25 `
  -DurationSecondsPerRate 300
```

Multi-step ramp:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa\run-connect-sink-ramp-suite.ps1 `
  -SuiteName flowplane-connect-byteconverter-ramp-25-50-75 `
  -Rates 25,50,75 `
  -DurationSecondsPerRate 120
```

Fast producer/sink evidence without probe extraction:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa\run-connect-sink-ramp-suite.ps1 `
  -SuiteName flowplane-connect-byteconverter-quick-no-probes `
  -Rates 25 `
  -DurationSecondsPerRate 120 `
  -SkipProbeCapture
```

## Metrics Interpretation

- `producer.p99Ms`: producer/client path latency from `kafka-producer-perf-test`.
- `transformProbe.coldInclusiveLast.p99Ms`: cumulative FLOWPLANE transform p99 including startup/warmup.
- `transformProbe.warmPost60sWindowMax`: warm upper-bound signal from runtime probe windows. This is not a true warm p99.
- `rawTotal`: records produced to the raw topic.
- `dlqTotal`: field-level failure events written to the DLQ topic.
- `residualLag`: connector consumer lag at the end of the catch-up window.
- `sink`: Mongo/Postgres row count or S3 object summary.

## Current Caveat

S3 sink commits offsets when files are committed. With `flush.size` only, sparse tail records can remain buffered across many partitions. For deterministic demo completion, configure a time-based rotation such as `rotate.interval.ms`, or lower `flush.size` for the run.
