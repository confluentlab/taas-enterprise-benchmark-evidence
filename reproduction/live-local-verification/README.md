# Isolated live-local verification runner

This folder upgrades the copied demo scripts into an evidence campaign without changing the Flowplane product repository, its completed demo video, or the public evidence repository.

## Safety boundary

- Product code and public evidence are read-only inputs.
- Generated fixtures, audits, logs, and run bundles stay under `video-generation-scripts-copy/artifacts/live-local-verification`.
- `-Execute` requires explicit integration IDs and never starts all runtimes implicitly.
- An integration can execute only through a reviewed adapter in `adapters/<integration>.ps1`.
- The verifier may publish only to the raw input destination. It must have no producer, credentials, or write path for transformed or DLQ destinations.
- The independently deployed integration must own transformed and DLQ writes after receiving the Flowplane runtime response.
- Kafka or broker topic screenshots are supporting evidence only. Each proof must also preserve a screenshot from the integration's own UI, dashboard, job view, or operational API wherever the integration exposes one.
- A zero exit code cannot assign `LIVE_LOCAL_VERIFIED`; the evaluator derives status from 26 gates, minimum record counts, exact accounting, and final state.
- No commit, push, or public-claim update is performed.

## Commands

Read-only audit and canonical synthetic fixture generation:

```powershell
powershell -File .\scripts\demo\11-run-live-local-verification.ps1
```

Prepare machine-readable `NOT_TESTED` bundles for selected paths:

```powershell
powershell -File .\scripts\demo\11-run-live-local-verification.ps1 `
  -PrepareRunBundles `
  -Integration kafka-connect,flink,grpc-batch
```

Execute only after the corresponding isolated adapters have been reviewed:

```powershell
powershell -File .\scripts\demo\11-run-live-local-verification.ps1 `
  -Execute `
  -Integration kafka-connect
```

The runtime-surface campaign uses the same command and accepts these IDs:

```powershell
powershell -File .\scripts\demo\11-run-live-local-verification.ps1 `
  -Execute `
  -Integration embedded-spring,http-single,http-batch,grpc-batch,grpc-streaming,serverless-aws,serverless-azure,serverless-gcp
```

`adapters/runtime-surface.ps1` creates and deploys a run-specific governed
mapping, pre-registers the runtime, starts the independently packaged runtime
or bridge, and delegates all input/output reconciliation to the Kafka
raw-only verifier. The gRPC streaming bridge uses one long-lived
bidirectional stream for the run rather than reducing streaming to repeated
unary calls.

## Adapter contract

An adapter receives `FlowplaneRoot`, `BundleRoot`, and `FixtureRoot`. It must cross the real runtime boundary and write:

- `actual/adapter-gate-assertions.json`
- `counts.json`
- `final-state.json`
- raw request/output material under `actual/`
- sanitized runtime logs under `sanitized-logs/`
- runtime metrics under `metrics/`
- exact executed commands in `commands.txt`
- runtime and image versions in `versions.json`

Every passed gate must cite a bundle-local evidence path. Non-applicable gates require a technical reason. Failed and partial attempts remain preserved and are never relabeled as successful.

Every adapter must also produce `actual/write-boundary-audit.json`. The
`boundary.verifierWritesRawOnly` gate passes only when that audit proves the
verifier has exactly one raw write target, no downstream write target, and the
integration pipeline owns both downstream destinations.

## Visual evidence contract

Use the strongest tool-native surface available:

- broker or streaming dashboards for Pulsar, NiFi, Spark, Flink, RabbitMQ,
  Redis, RocketMQ, EMQX, and similar systems;
- Redpanda Console consumer-group or pipeline views for Redpanda Connect;
- the live Logstash node/pipeline API when Kibana monitoring is not deployed;
- framework runtime or job telemetry for Camel, Beam, and Spring Cloud Stream.
- Flowplane's mapping and runtime monitor pages plus native runtime logs for
  protocol/serverless surfaces that do not ship a provider dashboard locally.

Destination-topic screenshots do not by themselves prove which integration
performed the work. The tool-native screenshot must show the run-specific
pipeline, consumer group, job, processor graph, or runtime identity.

## Current audit behavior

The inventory maps public claims to actual local modules and harnesses. Pulsar,
Debezium, and the broker-sidecar candidates now point to their copied executable
adapters and preferred proof bundles. Missing or failing paths remain visibly
non-runnable, and an existing small-fixture or contract result is not
automatically promoted to Pulsar-grade streaming evidence.

The current preferred runtime-surface bundles are all `LIVE_LOCAL_VERIFIED`.
The serverless scope is the HTTP wrapper: AWS API Gateway-style events through
Lambda RIE, the Azure HTTP trigger in the official Java Functions image, and
the GCP HTTP function in Java Functions Framework `2.0.1`. Azure Queue/Event
Hub triggers and the GCP Pub/Sub CloudEvent handler need separate event-trigger
qualifications.

## Pulsar pipeline boundary

The Pulsar adapter requires the persistent local deployment in
`video-generation-scripts-copy/pulsar-local-ui` and starts two run-specific
containers:

1. A Flowplane Bento HTTP sidecar with the approved artifact loaded.
2. An independent Pulsar pipeline that consumes the raw topic, calls the
   sidecar, acknowledges input only after a successful downstream publish, and
   writes transformed or DLQ records.

The verifier has one producer and it targets only the raw topic. It opens
read-only consumers for the transformed and DLQ topics. A generated
`actual/write-boundary-audit.json` records and enforces that separation.
