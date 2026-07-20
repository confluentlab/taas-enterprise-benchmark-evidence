# Frequently asked questions

## Does FlowPlane move production payloads into its control plane?

The intended architecture executes payload transformations in customer-owned runtimes. The control plane distributes versioned artifacts and receives bounded telemetry and failure metadata. Explicit replay or retention features can widen that boundary and are gated and off by default where payload storage is involved.

## Is every listed runtime production-certified?

No. The portability matrix separates native modules, assigned adapters, stateless APIs, contract verification, live local verification, incomplete runs, and preserved failures. No vendor certification is claimed.

## Why publish a rejected benchmark?

Because repeatability variance is evidence. Preserving the measured values, thresholds, and observations makes later improvements auditable and prevents selection bias. The benchmark completed; its cross-run stability qualification was not met.

## Are JMH and live Flink latency comparable?

No. JMH isolates the engine’s parse/map/serialize boundary; the live proof adds runtime and transport effects.

## Can I reproduce the evidence exactly?

The repository preserves inputs, metrics, environment details, methodology, and hashes needed to audit the published claims. Exact timing will vary by host and runtime. The source revision inspected was `10a26df4d7ed6a41f8076a5d7280d73db543c13a`.
