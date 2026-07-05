# FLOWPLANE Core Production Integration Notes

This module now has a production-ready path for the optimized mapping runtime with
rollback controls, runtime plan introspection, concurrency coverage, and benchmark
gates.

## Runtime Entry Point

Use compiled mappings as shared, long-lived objects:

```java
CompiledMapping mapping = FlowPlane.compile(mappingDsl);
TransformResult result = mapping.transformHybrid(payloadBytes, context, RuntimeOutputOptions.defaults());
```

`CompiledMapping` keeps mutable executors in `ThreadLocal` storage, so one compiled
mapping can be reused by concurrent request threads without sharing extraction
buffers or hash scratch state between requests.

## Parser Rollout Switch

Selective payload parsing can avoid Jackson field-name canonicalization for
scan-heavy mappings:

```text
-Dflowplane.json.uncanonicalizedSelective=auto
```

Supported values:

```text
auto    enable only for selective scan-heavy access plans
never   production rollback switch
always  force for comparative benchmark runs
```

Start production rollout with `auto`. If a production issue appears, switch to
`never` without changing application code.

## Runtime Plan Introspection

Wrappers can log the selected plan at mapping load time:

```java
RuntimePlanInfo plan = mapping.runtimePlan(RuntimeOutputOptions.defaults());
```

The plan exposes the parse strategy, output shape, field count, selective parsing
state, uncanonicalized-field-name decision, scan/index/field plan counts, and
streaming-simple-extractor eligibility.

## Correctness Gate

Before promoting a mapping/runtime pair, run the focused suite:

```powershell
mvn -Dtest='FlowPlaneCoreTest,ParseStrategyLabTest,ParseStrategyAllOperationsEquivalenceTest' test
```

The all-operations equivalence suite includes:

- full/selective/hybrid parse equivalence
- byte and stream payloads
- output materialization options
- expected conversion error paths
- shared compiled mapping concurrency
- parser mode equivalence for `auto`, `never`, and `always`

## Benchmark Gate

For quick CI or pre-prod checks, use short runs first:

```powershell
$cp="target/test-classes;target/classes;$(Get-Content -LiteralPath target/core-cp.txt)"
java -Xms2g -Xmx2g -XX:+UseG1GC -cp $cp com.flowplane.core.FlowPlaneCoreOptimizationExperiment `
  --run-root ../benchmarks/flowplane-core/bench-runs/flowplane-core-optimization/20260611-234936 `
  --phase production-gate `
  --payloads 10 `
  --duration-seconds 8 `
  --warmup-cycles 5 `
  --correctness-payloads 2 `
  --require-correctness `
  --max-p50-ms 7.5 `
  --max-p95-ms 10.0 `
  --max-p99-ms 12.0 `
  --max-allocation-ratio 0.50
```

The harness writes:

- `metrics/<phase>-runtime-plan.json`
- `metrics/<phase>-summary.json`
- `metrics/<phase>-latencies.csv`
- `metrics/<phase>-cycles.csv`
- `metrics/<phase>-heap-windows.csv`

Use longer soak runs only after the short gate passes.

## Concurrent Shared-Mapping Demo

Use this shape to model a production service reusing one compiled mapping across
many worker threads:

```powershell
$cp="target/test-classes;target/classes;$(Get-Content -LiteralPath target/core-cp.txt)"
java -Xms2g -Xmx2g -XX:+UseG1GC -cp $cp com.flowplane.core.FlowPlaneCoreOptimizationExperiment `
  --run-root ../benchmarks/flowplane-core/bench-runs/flowplane-core-optimization/20260611-234936 `
  --phase demo-5min-50threads-50payloads `
  --payloads 50 `
  --threads 50 `
  --duration-seconds 300 `
  --warmup-cycles 5 `
  --correctness-payloads 3 `
  --require-correctness `
  --max-allocation-ratio 0.60
```

This rotates each worker across all 50 same-structure payload files while sharing
one `CompiledMapping` instance. The harness records per-operation latency,
per-operation thread allocation, per-thread summaries, passive heap windows, GC
deltas, runtime plan details, and correctness comparisons.

For Kafka Connect SMT-style task modeling, use one compiled mapping per worker:

```powershell
$cp="target/test-classes;target/classes;$(Get-Content -LiteralPath target/core-cp.txt)"
java -Xms2g -Xmx2g -XX:+UseG1GC -cp $cp com.flowplane.core.FlowPlaneCoreOptimizationExperiment `
  --run-root ../benchmarks/flowplane-core/bench-runs/flowplane-core-optimization/20260611-234936 `
  --phase connect-style-5min-50tasks-50payloads `
  --payloads 50 `
  --threads 50 `
  --mapping-scope per-thread `
  --duration-seconds 300 `
  --warmup-cycles 5 `
  --correctness-payloads 3 `
  --require-correctness `
  --max-allocation-ratio 0.60
```

This compiles one mapping instance per worker before the timed transform window,
then rotates each worker across the 50 payloads.
