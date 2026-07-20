# Architecture

FlowPlane separates governance from payload execution.

- **Control plane:** mapping lifecycle, simulation, approvals, immutable artifacts, deployment orchestration, RBAC, audit, telemetry ingestion, and operator UI.
- **Runtime contract:** stable runtime/deployment/instance identity, capabilities, assignment polling, heartbeat, deployment status, latency/throughput/heap/GC/lag/error metrics, canonical failures, replay, and schema reporting.
- **Customer data plane:** embedded SDKs, native runtime modules, sidecars, or wrappers that fetch artifacts and transform records without sending production payloads to the control plane.
- **Evidence plane:** reproducible reports, raw measurements, environment metadata, hashes, and explicit pass/fail gates.

Artifacts move from the control plane to runtimes. Bounded telemetry and failure metadata move back. Payloads remain within the runtime boundary unless an operator explicitly enables a retention or replay path.

See [Runtime portability](runtime-portability.md) and [Failure handling](failure-handling.md).
