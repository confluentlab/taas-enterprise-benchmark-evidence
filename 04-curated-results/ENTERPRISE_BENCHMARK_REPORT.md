# Enterprise Benchmark Report

## Executive Summary

The local benchmark evidence supports FLOWPLANE as a production-candidate stateless transformation runtime for Kafka-centric streaming workloads.

The strongest result is the isolated 100 rps per-sink Connect SMT run with 50 rotating unique payloads of about 1 MB each. That test produced about 99 MB/sec ingress locally and kept FLOWPLANE transform p99 low:

- Mongo: 6.576 ms p99.
- Postgres/JDBC: 2.861 ms p99.
- S3/MinIO: 6.432 ms p99.

At this same rate, local Docker Connect/sink drain accumulated lag. That means the local bottleneck was not FLOWPLANE transform time; it was sink and infrastructure throughput under local resource limits.

## Key Evidence

| Evidence | Path |
| --- | --- |
| 50 unique payload corpus | `01-sample-payloads/` |
| 2000-field scenario contract | `02-mappings-and-configs/scenario-summary.json` |
| Control-plane mapping export | `02-mappings-and-configs/control-plane-mappings-export.json` |
| Control-plane draft export | `02-mappings-and-configs/control-plane-mapping-drafts-export.json` |
| 100 rps Connect sink run | `03-raw-benchmark-runs/flowplane-connect-byteconverter-100rps-3min-20260613-031931` |
| 50 rps Connect sink runs | `03-raw-benchmark-runs/flowplane-connect-byteconverter-50rps-5min-20260613-023744` and `03-raw-benchmark-runs/flowplane-connect-byteconverter-50rps-5min-postgres-s3-20260613-025043` |
| 50 unique Connect/Flink run | `03-raw-benchmark-runs/flowplane-1mb-connect-flink-50unique-50rps-5min-20260612112658` |
| 30 minute stability run | `03-raw-benchmark-runs/flowplane-1mb-50unique-50rps-30min-20260612-122436` |

## Interpretation

FLOWPLANE Core appears strong for the tested stateless transform workload:

- Large payload handling worked.
- Deep nesting worked.
- Large mapping shape was represented in the scenario contract.
- Failure path and DLQ path were exercised.
- Transform latency remained low relative to 1 MB payload size and mapping complexity.
- Allocation in the measured transform path was stable.

The platform still needs production-style validation:

- Multi-worker Connect deployment.
- Multi-node Kafka cluster.
- Larger Flink parallelism.
- More realistic partition counts.
- Production-grade sink capacity.
- Cloud or enterprise cluster quotas.
- Warm-only p99 emitted directly by the probe instead of inferred from cumulative logs.

## Recommended Claim

FLOWPLANE is ready to be described as:

> A governed zero-code transformation layer that replaces stateless custom Kafka consumer transformation logic and runs through Kafka Connect, Flink, and Kafka Streams wrappers.

For ETL:

> FLOWPLANE plus Flink and Kafka Connect can serve as a practical streaming ETL transformation platform for stateless, policy-governed Kafka workloads.

