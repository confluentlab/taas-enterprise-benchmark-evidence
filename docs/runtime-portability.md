# Runtime portability

This matrix reflects the preserved local evidence through 2026-07-21 UTC. “Native” describes an implementation module, not vendor certification. “HTTP/sidecar” means the named tool transported a record to or from a separately deployed Flowplane runtime.

| Runtime path | Implementation | Latest preserved local evidence |
|---|---|---|
| Embedded Java / Spring Boot | Native embedded engine | `LIVE_LOCAL_VERIFIED`; baseline plus a 10,000-record/five-minute soak |
| Kafka Connect SMT | Native connector transform | `MEASURED_NOT_QUALIFIED`; exact content accounting passed, broker-derived final-lag rows absent |
| Kafka Streams | Native topology integration | `LIVE_LOCAL_VERIFIED`; focused 100-valid/10-invalid Docker proof |
| Apache Flink | Native map/runtime integration | Focused proof `MEASURED_NOT_QUALIFIED` for missing broker-derived lag rows; separate 1 MiB live probe remains `LIVE_LOCAL_VERIFIED` |
| HTTP single | Stateless synchronous sidecar API | `LIVE_LOCAL_VERIFIED`; baseline plus a 10,000-record/five-minute soak |
| HTTP batch | Stateless batch sidecar API | `LIVE_LOCAL_VERIFIED`; baseline plus a 10,000-record/five-minute soak |
| gRPC batch | Live sidecar `TransformBatch` | `LIVE_LOCAL_VERIFIED`; baseline plus a 10,000-record/five-minute soak |
| gRPC bidirectional stream | Live sidecar `TransformStream` | `LIVE_LOCAL_VERIFIED`; baseline plus a 10,000-record/five-minute soak |
| AWS Lambda HTTP wrapper | Assigned wrapper in Docker | `LIVE_LOCAL_VERIFIED`; Lambda Runtime Interface Emulator |
| Azure Functions HTTP wrapper | Assigned wrapper in Docker | `LIVE_LOCAL_VERIFIED`; official Azure Functions Java host image |
| Google Cloud Functions HTTP wrapper | Assigned wrapper in Docker | `LIVE_LOCAL_VERIFIED`; Java Functions Framework 2.0.1 image |
| Azure QueueTrigger | Provider trigger handler | `LIVE_LOCAL_VERIFIED — emulator`; Azurite input/output queues |
| Azure EventHubTrigger | Provider trigger handler | `LIVE_LOCAL_VERIFIED — emulator`; Microsoft Event Hubs emulator plus Azurite DLQ |
| GCP Pub/Sub CloudEvent | Provider CloudEvent handler | `LIVE_LOCAL_VERIFIED — emulator`; Pub/Sub emulator plus documented envelope-only bridge |
| Pulsar, ActiveMQ Classic/Artemis, NATS, Redis Streams, RabbitMQ Streams, EMQX/MQTT, RocketMQ | Broker-native input/output plus Flowplane HTTP/sidecar execution | `LIVE_LOCAL_VERIFIED` for focused local Docker fixtures |
| Redpanda Connect, Logstash, Camel, Spring Cloud Stream, NiFi, Spark, Beam, Bento, Vector, OpenTelemetry Collector, Debezium | Tool/framework integration, primarily HTTP or sidecar | Spark and Beam are `MEASURED_NOT_QUALIFIED` for missing broker-derived lag rows; the remaining focused fixtures are `LIVE_LOCAL_VERIFIED` |

The runtime contract includes stable identity, capabilities, assignment polling, heartbeat, deployment state, bounded operational metrics, canonical failure records, replay, and schema observations. See [Runtime parity](../evidence/runtime-parity/summary.md) for the earlier fixed-fixture contract proof and [Proven local integration evidence](../evidence/integration-proofs/EVIDENCE-OVERVIEW.md) for live execution boundaries, run IDs, screenshots, and raw artifacts.

## Important distinctions

- Implementation presence is not execution proof; every green row above links to a preserved successful bundle.
- Local Docker/emulator evidence is not managed-cloud parity or vendor certification.
- A tool can be interoperable through HTTP/sidecar transport without being a first-class native Flowplane wrapper.
- Historical incomplete HTTP and `UNIMPLEMENTED` gRPC attempts are preserved, but newer successful runs are now the latest local evidence.
