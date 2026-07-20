# Benchmark methodology

## Evidence classes

1. **Core microbenchmark:** JMH measures raw input bytes through parse/scan, 976 compiled mapping fields, transformations/policies/error handling, and serialization to owned bytes. Mapping compilation and corpus setup are outside the timed region.
2. **Payload-scaling benchmark:** the same compiled mapping and fixed normalized output are exercised from 1 to 64 MiB. The added bulk field is deliberately unreferenced.
3. **Live runtime probe:** Kafka and Flink run locally in Docker, including transport and runtime execution. These latencies are not directly comparable to JMH.
4. **Soak:** a 30-minute producer/consumer run preserves producer, streaming, topic-count, lag, throughput, latency, heap, and GC evidence.
5. **Contract parity:** fixed valid and invalid fixtures are executed through protocol adapters and compared by output hash and canonical failure fields.

## Core protocol

The 1 MiB publication candidate used 12 controlled launches with 30-second cooldowns on Windows 11, Oracle JDK 21.0.9, HotSpot/G1, 16 logical cores, approximately 33.82 GB RAM, AC power, and the Balanced power plan. The exact input was 1,049,487 bytes with 100 variants and 976 compiled mapping fields. Raw hashes were verified and the associated pre-publication semantic suite passed 174 tests. Nine of twelve repeatability checks met their thresholds; see [Repeatability qualification](repeatability-qualification.md).

“Full scan” means the parser/scanner consumes the complete input byte sequence. It does not mean every input field is materialized into the output model.

## Scaling protocol

Average time used 5 × 1-second warmups, 5 × 10-second measurements, and 3 forks. Allocation used 5 × 1-second warmups, 3 × 10-second measurements, and 3 forks. Checkpoint percentiles used 5 × 1-second warmups and one 60-second measurement across 3 forks. JVM flags included `-Xms4g -Xmx4g -XX:+UseG1GC -XX:+AlwaysPreTouch`.

## Interpretation rules

- Preserve missed qualification checks with their threshold and observation; do not relabel a measured result as qualified.
- Compare results only when timed boundaries, payload shape, output materialization, host, runtime, and protocol are compatible.
- Treat local Docker, LocalStack, and emulator runs as local proofs, not managed-service certification.
- Verify all public artifacts with [checksums](../evidence/checksums.sha256).
