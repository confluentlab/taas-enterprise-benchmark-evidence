# FLOWPLANE Enterprise Benchmark Evidence

This folder is a clean evidence package for the local FLOWPLANE hardening and benchmark work performed against the 1 MB payload scenario.

It contains the raw benchmark outputs, payload corpus, runtime/connector configs, control-plane mapping exports, orchestration scripts, and a readable interpretation of what the results prove.

## What Problem FLOWPLANE Solves

Enterprise streaming teams often write custom Kafka consumer applications just to do repetitive transformation work:

- Parse large JSON records.
- Extract deeply nested fields.
- Normalize names and shapes.
- Cast values into target types.
- Apply validation and policy rules.
- Mask, hash, or redact sensitive data.
- Route bad records to DLQ.
- Produce sink-ready records for Mongo, JDBC, S3, warehouses, or downstream topics.

That custom code becomes expensive to maintain. Every field change, sink change, policy change, or schema adjustment usually means code edits, tests, image rebuilds, deployment work, and separate observability wiring.

FLOWPLANE moves that work into a governed platform:

- The control plane owns mappings, lifecycle, auditability, simulation, governance, runtime registration, and failure visibility.
- FLOWPLANE Core executes compiled mappings as a stateless Java transformation engine.
- Runtime wrappers run the same transformation contract inside Kafka Connect SMT, Flink, Kafka Streams, or Spring Boot style runtimes.
- Sink systems remain normal infrastructure: Connect sinks, Flink sinks, object stores, databases, or downstream Kafka topics.

The point is not to replace every ETL product in the universe. The point is sharper and more defensible:

> FLOWPLANE replaces stateless custom Kafka consumer transformation logic with a governed, zero-code, runtime-portable transformation layer.

## Architecture

```text
                         +-----------------------+
                         | FLOWPLANE Control Plane    |
                         |-----------------------|
                         | mappings              |
                         | versions              |
                         | governance            |
                         | runtime registry      |
                         | audit logs            |
                         | simulation            |
                         | field failures        |
                         +-----------+-----------+
                                     |
                                     | assignments, policies, mappings
                                     v
+-------------+        +-------------+-------------+        +------------------+
| Kafka topic | -----> | Runtime wrappers           | -----> | Sink/output       |
| raw input   |        |----------------------------|        |------------------|
|             |        | Kafka Connect SMT          |        | MongoDB sink      |
|             |        | Flink job                  |        | JDBC/Postgres     |
|             |        | Kafka Streams wrapper      |        | S3/MinIO          |
|             |        | Spring Boot wrapper        |        | downstream Kafka  |
+-------------+        +-------------+-------------+        +------------------+
                                     |
                                     v
                         +-----------------------+
                         | FLOWPLANE Core             |
                         |-----------------------|
                         | compiled mapping      |
                         | selective JSON access |
                         | field extraction      |
                         | type enforcement      |
                         | policies              |
                         | output materializer   |
                         | field-level errors    |
                         +-----------------------+
```

## ETL Positioning

FLOWPLANE alone is a stateless transformation engine and governance control plane.

FLOWPLANE plus Kafka Connect gives a sink-oriented streaming transformation layer:

- Kafka Connect handles source/sink integration.
- FLOWPLANE SMT handles transformation, validation, policy, and output materialization.
- Existing Connect sinks load to Mongo, JDBC/Postgres, S3/MinIO, and similar connector families.

FLOWPLANE plus Flink gives a scalable streaming ETL runtime:

- Flink provides distributed stream execution, scaling, checkpointing, restart behavior, and richer stream processing foundations.
- FLOWPLANE provides zero-code transformation logic, governance, policies, and consistent field-level behavior.
- Flink can emit transformed topics or write through Flink connectors/sinks.

The strongest honest claim is:

> FLOWPLANE plus Flink and Kafka Connect can operate as a practical streaming ETL platform for stateless, policy-governed record transformation and sink preparation.

Avoid overclaiming it as a full replacement for batch orchestration, CDC platforms, catalog/lineage products, warehouse modeling tools, or every class of stateful application logic.

## Evidence Folder Layout

```text
00-overview/
  evidence-inventory.json

01-sample-payloads/
  source-payload-1mb.json
  source-payloads-50unique-1mb.jsonl
  payloads-50/
  payloads-manifest.csv
  payload-corpus-summary.txt

02-mappings-and-configs/
  control-plane-mappings-export.json
  control-plane-mapping-drafts-export.json
  scenario-summary.json
  connector configs and run-start metadata

03-raw-benchmark-runs/
  raw copied benchmark directories

04-curated-results/
  top-level summaries copied from benchmark runs

05-orchestration-scripts/
  repeatable PowerShell harnesses and runbooks

06-runtime-validation/
  production integration notes

07-notes-and-caveats/
  reserved for follow-up analysis notes
```

## Scenario Under Test

The core scenario is recorded in:

`02-mappings-and-configs/scenario-summary.json`

Key setup:

- Payload size: `1,039,980` bytes per record, just under 1 MB.
- Payload shape: deeply nested JSON, including 10-level nesting and repeated nested records.
- Source corpus: 50 unique payload variants with the same schema and different data.
- Unique hashes: 50.
- Fields per mapping: 2000 for the main breakpoint mappings.
- Postgres mapping size: 300 fields for the JDBC-oriented test.
- Runtimes:
  - Kafka Connect SMT for Mongo.
  - Kafka Connect SMT for Postgres/JDBC.
  - Kafka Connect SMT for S3/MinIO.
  - Flink runtime.
- Connect tasks: 3.
- Flink parallelism: 2.
- DLQ topics were separated per runtime/sink.

Payload corpus proof:

- `01-sample-payloads/payload-corpus-summary.txt`
- `01-sample-payloads/payloads-manifest.csv`
- `01-sample-payloads/payloads-50/`
- `01-sample-payloads/source-payloads-50unique-1mb.jsonl`

## Important Test Runs

### 50 Unique Payload 5 Minute Connect and Flink Run

Path:

`03-raw-benchmark-runs/flowplane-1mb-connect-flink-50unique-50rps-5min-20260612112658`

Summary:

- Raw records: 15,000.
- Flink success: 9,931.
- Inferred failed source records: 5,069.
- Connect DLQ events: 20,276.
- Flink DLQ events: 20,276.
- Connect probe lines: 206.
- Flink probe lines: 177.

Significance:

- This proves both Connect SMT and Flink wrappers processed the same rotating 50-payload corpus.
- Failure behavior was exercised, not just happy path.
- The system continued after field failures and produced DLQ evidence instead of crashing.

### 50 Unique Payload 30 Minute Stability Run

Path:

`03-raw-benchmark-runs/flowplane-1mb-50unique-50rps-30min-20260612-122436`

Summary:

- Raw records: 90,000.
- Flink output offsets after run: 14,957.
- Connect DLQ offsets after run: 39,595.
- Flink DLQ offsets after run: 30,647.

Significance:

- This is the stronger platform stability signal.
- It ran longer than the quick smoke tests and kept exercising success plus failure paths.
- The evidence shows high DLQ volume without runtime collapse.

### Connect ByteArrayConverter 100 RPS, 3 Minutes Per Sink

Path:

`03-raw-benchmark-runs/flowplane-connect-byteconverter-100rps-3min-20260613-031931`

Summary:

- Mode: isolated connector runs.
- Rate: 100 records/sec.
- Payload: 50 rotating unique payloads, each about 1 MB.
- Ingress: about 99 MB/sec producer rate.
- Records per sink run: 18,000.
- Connector sinks tested: Mongo, Postgres/JDBC, S3/MinIO.

Transform p99:

| Sink | Transform p50 | Transform p95 | Transform p99 | Max | p99 allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mongo | 3.846 ms | 5.312 ms | 6.576 ms | 24.839 ms | 2,667,248 bytes |
| Postgres | 1.396 ms | 2.329 ms | 2.861 ms | 16.796 ms | 1,434,504 bytes |
| S3 | 4.026 ms | 5.645 ms | 6.432 ms | 21.785 ms | 2,667,248 bytes |

Producer path:

| Sink run | Producer rate | Producer p99 | Producer p99.9 |
| --- | ---: | ---: | ---: |
| Mongo | 99.999 rps / 99.18 MB/sec | 12 ms | 50 ms |
| Postgres | 99.998 rps / 99.18 MB/sec | 9 ms | 44 ms |
| S3 | 99.996 rps / 99.18 MB/sec | 9 ms | 26 ms |

Significance:

- FLOWPLANE transform cost stayed low even under a 1 MB payload and 100 rps local ingress.
- The p99 allocation was stable and bounded for the measured transform path.
- Local Docker sinks accumulated residual lag at this rate. That is important: the bottleneck was Connect/sink/Kafka drain capacity under local machine pressure, not FLOWPLANE Core transform execution.

### Connect ByteArrayConverter 50 RPS, 5 Minutes Per Sink

Paths:

- `03-raw-benchmark-runs/flowplane-connect-byteconverter-50rps-5min-20260613-023744`
- `03-raw-benchmark-runs/flowplane-connect-byteconverter-50rps-5min-postgres-s3-20260613-025043`

Summary:

- Mongo: 15,000 raw records.
- Postgres: 15,000 raw records.
- S3: 15,000 raw records.
- Payload: 50 rotating unique payloads, about 1 MB each.

Representative warm/cold transform values:

- Mongo warm log tail showed p99 around 7.25 to 7.43 ms with stable p99 allocation around 2,667,248 bytes.
- Postgres p99 around 2.877 ms with p99 allocation around 1,434,504 bytes.
- S3 p99 around 6.814 ms with p99 allocation around 2,667,248 bytes.

Significance:

- This is the cleaner local Docker rate than 100 rps for sink behavior.
- It confirms the warm transform path remains small and stable.
- It also shows why reports should separate transform latency from end-to-end pipeline and sink drain behavior.

### Allocation Probe Demo

Path:

`03-raw-benchmark-runs/flowplane-alloc-probe-demo-clean-10rps-20260613-0605`

Summary:

- Rate: 10 records/sec.
- Duration: 60 seconds.
- Corpus: same 50 unique 1 MB payloads.
- Purpose: demonstrate per-record transform allocation probe across runtimes/wrappers.

Significance:

- Allocation probes are transform-path probes, not whole-pipeline allocation.
- They are useful for comparing FLOWPLANE wrapper behavior, but they do not include all Kafka client, Connect framework, sink connector, broker, or database allocation.

## Output Type and Sink Compatibility Evidence

The tested Connect SMT output modes and sink families support this positioning:

- `SCHEMALESS_MAP` for document-style sinks such as Mongo.
- `STRUCT` for schema-aware sinks such as JDBC/Postgres.
- `STRING` and `BYTES` for object storage, log, HTTP, and raw payload-style sinks.
- Primitive output for scalar projection use cases.

Recent validation also confirmed Connect STRUCT logical type materialization:

- Decimal maps to Kafka Connect Decimal.
- Timestamp maps to Kafka Connect Timestamp.
- Date maps to Kafka Connect Date.
- Time maps to Kafka Connect Time.

That matters for strict JDBC and warehouse-style sinks, because they need stable field names and stable field types rather than generic strings/numbers.

## What The Results Prove

The evidence supports these claims:

- FLOWPLANE can process deeply nested, near-1 MB JSON payloads.
- FLOWPLANE can execute large mappings, including the 2000-field scenario contract.
- FLOWPLANE can run through Kafka Connect SMT and Flink wrappers.
- FLOWPLANE can handle happy path and field-failure path without stopping the runtime.
- FLOWPLANE can generate DLQ/failure evidence while continuing to process records.
- FLOWPLANE transform p99 stayed in single-digit milliseconds in the best measured Connect sink runs at 100 rps isolated local load.
- FLOWPLANE transform allocation stayed stable for the measured transform path.
- Kafka Connect and sink drain behavior became the practical local bottleneck at high ingress rates.

## What The Results Do Not Prove Yet

This package is strong local evidence, but not a full enterprise certification.

It does not yet prove:

- Multi-node Kafka production cluster behavior.
- Cloud Kafka limits or Confluent Cloud quota behavior.
- Schema Registry compatibility across every Avro/Protobuf/JSON Schema mode.
- Exactly-once guarantees across all sinks.
- Stateful ETL behavior like joins, windows, aggregations, or cross-record correlation.
- External API or database lookup calls inside the transform path.
- Certified support for every Kafka Connect connector.

The correct enterprise language is:

> Production-candidate local evidence for stateless streaming transformation and sink preparation, with further validation required on representative production infrastructure.

## Honest Conclusion

FLOWPLANE is not just a demo. The evidence shows a real platform pattern:

- A control plane governs mappings and runtime assignments.
- FLOWPLANE Core executes stateless transformations.
- Connect and Flink wrappers allow the same transformation logic to run in standard streaming infrastructure.
- Large payloads, many fields, failures, DLQ, and multiple sink types were all exercised.

The most credible product position is:

> FLOWPLANE is a governed, zero-code transformation layer for Kafka, Connect, and Flink that can replace a large class of stateless custom consumer services.

And when combined with Flink and Kafka Connect:

> FLOWPLANE can operate as a practical streaming ETL transformation platform for Kafka-centric architectures.

The local tests also show where to be careful:

- End-to-end latency is not the same as FLOWPLANE transform latency.
- Sink lag at high local rates reflects local Docker, Connect, Kafka, and sink capacity.
- Allocation probe numbers are transform-path numbers, not total JVM or cluster allocation.
- Full production claims require cluster-level testing with production-like Kafka partitions, Connect workers, Flink task managers, sink capacity, and observability.

## How To Reproduce Or Extend

Use the scripts in:

`05-orchestration-scripts/`

Important scripts:

- `run-1mb-platform-run.ps1`
- `monitor-1mb-platform-run.ps1`
- `run-1mb-ramp-suite.ps1`
- `run-connect-sink-ramp-suite.ps1`
- `CONNECT_SINK_RAMP_RUNBOOK.md`

Recommended next benchmark standards:

1. Always use unique topics and unique sink collections/tables/prefixes per run.
2. Record run start/end metadata.
3. Record connector status before and after the run.
4. Record producer report, transform probe report, DLQ counts, sink counts, and residual lag.
5. Separate cold-inclusive p99 from warm-post-60s p99.
6. Separate FLOWPLANE transform latency from producer latency and end-to-end pipeline latency.
7. Keep raw artifacts immutable once a report is written.

## Best Public Claim

Use this:

> FLOWPLANE is a governed zero-code transformation layer for Kafka, Flink, and Kafka Connect. It replaces stateless custom Kafka consumer transformation code for parsing, mapping, validation, policy enforcement, DLQ routing, and sink preparation.

Use this when talking about ETL:

> FLOWPLANE plus Flink and Kafka Connect provides a practical streaming ETL platform for stateless, policy-governed record transformation.

Avoid this unless future evidence expands:

> FLOWPLANE is a complete generic ETL replacement for every batch, CDC, orchestration, catalog, lineage, and stateful processing tool.

