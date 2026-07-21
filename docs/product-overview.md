# Product overview

FlowPlane provides a governed lifecycle for field-level transformations across separately deployed streaming data-plane runtimes. A control plane manages drafts, simulation, review, publication, rollout, telemetry, failure workflows, and rollback. The Java execution engine applies immutable mapping artifacts inside the data plane; runtime ownership and hosting are deployment-specific.

The current implementation includes a Spring Boot control plane, React operator console, Java runtime contract/client/executor, native modules for Kafka Connect, Kafka Streams, Flink, and Spring, plus HTTP, gRPC, Bento, sidecar, and serverless integration paths.

Production promotion remains gated on repeatable evidence from the intended target environment. Start with [How it works](how-it-works.md), [Architecture](architecture.md), and the [Evidence index](../evidence/evidence-index.md).
