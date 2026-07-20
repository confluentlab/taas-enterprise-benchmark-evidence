# Benchmark interpretation

The observed core-engine mean of about 0.5 ms for a 1 MiB fixture is useful engineering evidence. The benchmark completed, but its status is `MEASURED_NOT_QUALIFIED`: 9 of 12 repeatability checks met their thresholds, while three cross-run mean-latency spreads exceeded the 5% tolerance. Correctness, output validation, operation completion, evidence hashes, percentile stability, and allocation stability did not fail.

The 1-64 MiB scaling result is almost linear for its frozen workload (R² 0.998895). Its approximately constant 189 KB/op allocation does **not** mean arbitrary 64 MiB transformations allocate only 189 KB. The generated bulk field was scanned but not referenced, and normalized output stayed at 36,478 bytes.

A separate realistic checkpoint referenced a 16 MiB field and materialized a 17.2 MB output. It took 178.765 ms and allocated 182,349,417 B/op. Output size and mapping behavior therefore dominate allocation when large values are copied or transformed.

Live Flink p50/p95/p99 measurements include runtime and transport effects that JMH excludes. Producer acknowledgement latency in the soak also reflects batching, broker behavior, and sustained pressure; it is not the transform-engine latency.

Use these results to understand the tested shapes, find regressions, and design target-environment qualification. Do not use them as universal throughput guarantees.
