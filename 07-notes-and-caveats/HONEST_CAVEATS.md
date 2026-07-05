# Honest Caveats

These benchmark artifacts are valuable, but they should be presented with precision.

## Transform Latency vs End-to-End Latency

FLOWPLANE transform latency is measured inside the runtime wrapper around the transform path. End-to-end latency includes producer, broker, Connect framework, sink connector, database/object storage, batching, flush behavior, and local resource pressure.

Do not mix these numbers.

## Allocation Scope

Allocation probe numbers are transform-path allocation numbers. They are not total JVM allocation for Kafka clients, Connect framework code, sink connector code, broker code, or database code.

## Local Docker Limits

High local ingress tests showed sink lag. That is expected in a single-machine Docker stack with Kafka, Connect, Flink, Mongo, Postgres, MinIO, UI, and backend competing for resources.

The lag does not invalidate FLOWPLANE transform performance, but it does limit what can be claimed about full pipeline throughput.

## Generic ETL Claim

The defensible wording is:

> FLOWPLANE plus Flink and Kafka Connect provides a practical streaming ETL transformation platform for stateless, policy-governed Kafka workloads.

Avoid claiming FLOWPLANE alone replaces every ETL category, especially batch orchestration, CDC management, lineage/catalog systems, stateful joins/windows, warehouse modeling, and arbitrary external API orchestration.

