# Pulsar and other runtime paths

Small local passes exist for a Pulsar HTTP bridge and WarpStream/Bento paths. NiFi, Spark, Redpanda Connect, Logstash, Vector, Camel, Beam, Spring Cloud Stream, Debezium, and OpenTelemetry paths have measured local evidence but did not all satisfy the full matrix pass criteria.

These paths primarily use HTTP adapters or sidecars and should not be described as first-class native wrappers unless a dedicated runtime module and qualification gate are added.
