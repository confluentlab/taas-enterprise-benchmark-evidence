# Proven local integration evidence

This index covers the successful local-machine evidence captured on 2026-07-20 and 2026-07-21 UTC. It supersedes the older summary wording for these specific execution paths. It does not erase the historical failed and incomplete attempts, which remain useful records of earlier binaries and configurations.

## What is proven

The repository now preserves **38 checksum-verified successful bundles**:

- 22 streaming and ecosystem tool integrations;
- eight Flowplane runtime or protocol surfaces;
- five separate 10,000-record, five-minute stability soaks for the core JVM, HTTP, and gRPC surfaces; and
- three provider-trigger paths using local emulators.

Across those bundles, the captured accounting is **53,630 attempted inputs**, **52,800 transformed outputs**, and **830 intentional DLQ outputs**, with zero reported duplicates or unexplained loss. The 30 baseline integration/runtime bundles each used 100 valid and 10 intentional-invalid records. The five stability bundles each used 9,900 valid and 100 intentional-invalid records. The three trigger bundles each used 100 valid and 10 intentional-invalid records.

`LIVE_LOCAL_VERIFIED` means the named runtime, broker, service, container, or emulator executed locally and met the acceptance gates recorded in that bundle. It does not mean vendor certification, managed-cloud parity, production readiness, or unrestricted performance qualification.

## Proof boundary: no downstream fixture insertion

The normal evidence boundary is:

```text
raw-only verifier
  -> persistent input destination
  -> independently running integration or runtime
  -> Flowplane transformation
  -> integration/runtime-owned transformed or DLQ destination
  -> independent read, accounting, and hash comparison
```

The verifier produces only to the input destination. It reads output and DLQ destinations after execution. Each integration bundle contains `actual/write-boundary-audit.json`; a passing audit records zero verifier downstream producer targets or calls.

For HTTP and gRPC surfaces, `kafka-runtime-surface-bridge.mjs` consumes the raw Kafka record, invokes the assigned Flowplane endpoint, and transports the **actual runtime response** to the transformed or DLQ Kafka topic. It does not load the mapping DSL, compute the expected transform, or manufacture expected output. Expected records are generated separately through control-plane simulation and compared afterward as canonical SHA-256 multisets.

The embedded Spring runtime writes its own Kafka output and DLQ records. Azure and GCP trigger handlers write their provider output and DLQ destinations. The GCP local Eventarc substitute converts only the Pub/Sub push envelope into a binary CloudEvent envelope; it performs no Flowplane transformation and has no output/DLQ publisher.

## Streaming and ecosystem tools

Every row below is a focused local Docker interoperability proof with 110 attempted inputs, 100 exact transformed outputs, 10 intentional DLQ outputs, and zero reported pending work or lag at completion.

| Integration | Execution path | Preferred run | Native visual or operational evidence |
|---|---|---|---|
| Apache Pulsar | Persistent Pulsar topics → independent pipeline → Flowplane HTTP sidecar → Pulsar topics | [`20260720T135944Z`](pulsar/runs/20260720T135944Z/summary.md) | [Pulsar Manager transformed topic](pulsar/runs/20260720T135944Z/screenshots/pulsar-manager-native-transformed-topic.png) |
| Redpanda Connect | Kafka-compatible input → Redpanda Connect HTTP processor → Kafka output/DLQ | [`20260720T160552Z`](redpanda-connect/runs/20260720T160552Z/summary.md) | [screenshots](redpanda-connect/runs/20260720T160552Z/screenshots/) and live component metrics |
| Logstash | Kafka input → Logstash HTTP filter/output path → Kafka output/DLQ | [`20260720T161054Z`](logstash/runs/20260720T161054Z/summary.md) | [screenshots](logstash/runs/20260720T161054Z/screenshots/) and Logstash pipeline API |
| Apache Camel | Kafka route → Flowplane HTTP → Kafka output/DLQ | [`20260720T161756Z`](camel/runs/20260720T161756Z/summary.md) | Runtime logs and consumer offsets; no first-party dashboard was packaged |
| Spring Cloud Stream | Spring Kafka binder → Flowplane HTTP → Kafka output/DLQ | [`20260720T163032Z`](spring-cloud-stream/runs/20260720T163032Z/summary.md) | Spring Boot runtime logs and Flowplane screenshots |
| Apache NiFi | NiFi Kafka flow → InvokeHTTP → Kafka output/DLQ | [`20260720T163618Z`](nifi/runs/20260720T163618Z/summary.md) | [NiFi flow canvas](nifi/runs/20260720T163618Z/screenshots/) and completion API status |
| Spark Structured Streaming | Kafka source → Spark micro-batches → Flowplane HTTP → Kafka output/DLQ | [`20260720T164322Z`](spark-structured-streaming/runs/20260720T164322Z/summary.md) | [Spark History Server screenshots](spark-structured-streaming/runs/20260720T164322Z/screenshots/) |
| Apache Beam DirectRunner | Bounded local Beam job → Flowplane runtime → Kafka output/DLQ | [`20260720T170558Z`](beam-directrunner/runs/20260720T170558Z/summary.md) | Maven/Beam `BUILD SUCCESS` logs; DirectRunner has no first-party dashboard |
| Kafka Connect SMT | Kafka Connect MirrorSourceConnector with the Flowplane SMT | [`20260720T171900Z`](kafka-connect/runs/20260720T171900Z/summary.md) | [Control Center connector screenshots](kafka-connect/runs/20260720T171900Z/screenshots/) and Connect REST status |
| Kafka Streams | Native Flowplane Kafka Streams topology | [`20260720T172502Z`](kafka-streams/runs/20260720T172502Z/summary.md) | [Control Center consumer-group screenshots](kafka-streams/runs/20260720T172502Z/screenshots/) |
| Apache Flink | Kafka source → Flowplane transform → Kafka sink | [`20260720T174759Z`](flink/runs/20260720T174759Z/summary.md) | [Flink JobManager screenshots](flink/runs/20260720T174759Z/screenshots/) and checkpoint records |
| WarpStream Bento | Kafka-compatible input → Bento assigned adapter → output/DLQ | [`20260720T175209Z`](bento-warpstream/runs/20260720T175209Z/summary.md) | [Bento screenshots](bento-warpstream/runs/20260720T175209Z/screenshots/) and component metrics |
| ActiveMQ Classic | STOMP/JMS queues → independent pipeline → Flowplane HTTP → queues | [`20260720T180530Z`](activemq-classic/runs/20260720T180530Z/summary.md) | [ActiveMQ console screenshots](activemq-classic/runs/20260720T180530Z/screenshots/) and Jolokia counts |
| NATS JetStream | Persistent streams/consumers → Flowplane HTTP → output streams | [`20260720T181423Z`](nats-jetstream/runs/20260720T181423Z/summary.md) | [JetStream monitoring screenshots](nats-jetstream/runs/20260720T181423Z/screenshots/) and `/jsz` data |
| Redis Streams | Consumer groups → Flowplane HTTP → transformed/DLQ streams | [`20260720T182351Z`](redis-streams/runs/20260720T182351Z/summary.md) | [RedisInsight screenshots](redis-streams/runs/20260720T182351Z/screenshots/) and `XLEN`/`XPENDING` data |
| RabbitMQ Streams | Stream queues → Flowplane HTTP → transformed/DLQ streams | [`20260720T183755Z`](rabbitmq-streams/runs/20260720T183755Z/summary.md) | [RabbitMQ Management screenshot](rabbitmq-streams/runs/20260720T183755Z/screenshots/) and queue counts |
| EMQX / MQTT | QoS 1 MQTT topics → independent pipeline → Flowplane HTTP → MQTT topics | [`20260720T185514Z`](emqx-mqtt/runs/20260720T185514Z/summary.md) | [EMQX dashboard screenshots](emqx-mqtt/runs/20260720T185514Z/screenshots/) and delivery counters |
| Apache RocketMQ | RocketMQ topics → independent pipeline → Flowplane HTTP → topics | [`20260720T192342Z`](rocketmq/runs/20260720T192342Z/summary.md) | [RocketMQ dashboard screenshots](rocketmq/runs/20260720T192342Z/screenshots/) and broker counters |
| ActiveMQ Artemis | Durable anycast queues → independent pipeline → Flowplane HTTP → queues | [`20260720T194319Z`](activemq-artemis/runs/20260720T194319Z/summary.md) | [Hawtio screenshot](activemq-artemis/runs/20260720T194319Z/screenshots/) and broker queue statistics |
| Vector | Kafka source → Vector remap/HTTP sink → publisher transport → Kafka output/DLQ | [`20260720T200000Z`](vector/runs/20260720T200000Z/summary.md) | GraphQL inventory, Prometheus counters, and runtime logs; no first-party dashboard |
| OpenTelemetry Collector | Kafka receiver → Collector pipeline/exporter → publisher transport → Kafka output/DLQ | [`20260720T200343Z`](opentelemetry/runs/20260720T200343Z/summary.md) | Collector health, Prometheus telemetry, and runtime logs; no first-party dashboard |
| Debezium CDC | Verifier database inserts → Debezium MySQL CDC → Flowplane runtime → Kafka output/DLQ | [`20260720T202552Z`](debezium/runs/20260720T202552Z/summary.md) | Kafka Connect REST, MySQL/CDC accounting, and runtime logs; no bundled dashboard |

## Flowplane runtime and protocol surfaces

These rows expand the execution-surface matrix; they are not additional streaming products.

| Surface | Baseline proof | Stronger proof or packaging result |
|---|---|---|
| Embedded Spring/JVM | [`20260721T050302Z`](embedded-spring/runs/20260721T050302Z/summary.md), 100 + 10 | [`20260721T054132Z`](embedded-spring/runs/20260721T054132Z/summary.md), 9,900 + 100 over 300.801 s |
| HTTP single | [`20260721T045008Z`](http-single/runs/20260721T045008Z/summary.md), `/v1/transform` | [`20260721T054134Z`](http-single/runs/20260721T054134Z/summary.md), 9,900 + 100 over 300.722 s |
| HTTP batch | [`20260721T044908Z`](http-batch/runs/20260721T044908Z/summary.md), `/v1/transform:batch` | [`20260721T054136Z`](http-batch/runs/20260721T054136Z/summary.md), 9,900 + 100 over 300.609 s |
| gRPC batch | [`20260721T045105Z`](grpc-batch/runs/20260721T045105Z/summary.md), live `TransformBatch` | [`20260721T054138Z`](grpc-batch/runs/20260721T054138Z/summary.md), 9,900 + 100 over 300.920 s |
| gRPC bidirectional streaming | [`20260721T045213Z`](grpc-streaming/runs/20260721T045213Z/summary.md), live `TransformStream` | [`20260721T054141Z`](grpc-streaming/runs/20260721T054141Z/summary.md), 9,900 + 100 over 301.327 s |
| AWS Lambda HTTP wrapper | [`20260721T050358Z`](serverless-aws/runs/20260721T050358Z/summary.md), 100 + 10 | Dockerized with the Lambda Runtime Interface Emulator |
| Azure Functions HTTP wrapper | [`20260721T045510Z`](serverless-azure/runs/20260721T045510Z/summary.md), 100 + 10 | Dockerized with the official Azure Functions Java host image |
| Google Cloud Functions HTTP wrapper | [`20260721T045957Z`](serverless-gcp/runs/20260721T045957Z/summary.md), 100 + 10 | Dockerized with Java Functions Framework 2.0.1 |

All five stability runs recorded zero final Kafka lag, zero duplicates, zero unexplained missing records, and only healthy periodic container/control-plane observations.

## Provider-trigger proofs

| Trigger | Local provider boundary | Result | Evidence |
|---|---|---|---|
| Azure QueueTrigger | Azurite input/output queues; Azure Functions handler owns output writes | 100 exact outputs + 10 contract-valid DLQ envelopes | [`20260721t054012z`](../trigger-proofs/azure-queue/runs/20260721t054012z/proof-manifest.json) |
| Azure EventHubTrigger | Microsoft Event Hubs emulator input; Azurite DLQ; Azure Functions handler owns output writes | 100 exact outputs + 10 contract-valid DLQ envelopes | [`20260721t055132z`](../trigger-proofs/azure-eventhub/runs/20260721t055132z/proof-manifest.json) |
| GCP Pub/Sub CloudEvent | Google Pub/Sub emulator; envelope-only local Eventarc bridge; Java handler owns output writes | 100 exact outputs + 10 contract-valid DLQ envelopes | [`20260721t060215z`](../trigger-proofs/gcp-pubsub/runs/20260721t060215z/proof-manifest.json) |

The trigger result claims are backed by each bundle’s `actual/content-validation.json`, which records 100/100 expected hash matches, 10/10 error-contract matches, zero duplicates, and `passed: true`.

## Screenshots and primary evidence

The imported integration/runtime bundles contain **62 screenshots**. Four additional composite control-plane screenshots are preserved under [`evidence/live-local-supplement/screenshots`](../live-local-supplement/screenshots/). Screenshots corroborate runtime identity and UI-visible state. The primary evidence remains the run manifest, exact outputs, content hashes, write-boundary audit, broker/provider counts, health samples, logs, image identities, and per-bundle SHA-256 manifest.

Some deployed tools do not ship a first-party web dashboard in this configuration. For Camel, Spring Cloud Stream, Beam DirectRunner, Vector, OpenTelemetry Collector, and Debezium, the bundles intentionally use native APIs, metrics, offsets, and runtime logs instead of a synthetic screenshot.

## Reproduce or audit

- [Reproduction guide](../../reproduction/README.md)
- [Copied harness source](../../reproduction/live-local-verification/README.md)
- [Evidence importer](../../scripts/import-live-local-integration-evidence.ps1)
- [Local-evidence validator](../../scripts/validate-live-local-evidence.py)
- [Captured execution matrix](../live-local-supplement/audit/execution-matrix.csv)

Each `reproduce.ps1` inside a run bundle is the capture-time command record and may contain host-specific absolute paths. Use the repository-level reproduction guide for a fresh checkout.

## Qualification limits

- Local Docker and emulator results are not managed-cloud or vendor certification.
- The GCP Pub/Sub emulator does not include Eventarc; the envelope-only bridge is part of the documented local test boundary.
- The WarpStream row qualifies the repository’s Bento adapter against a local Kafka-compatible API, not the WarpStream broker/control plane.
- The five 10,000-record soaks qualify only the named JVM/HTTP/gRPC surfaces, fixture, host, and five-minute window.
- Earlier HTTP and gRPC failures remain under `evidence/historical-attempts/`; they describe historical artifacts and no longer represent the latest successful local runs.
