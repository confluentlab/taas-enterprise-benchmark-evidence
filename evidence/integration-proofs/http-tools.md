# HTTP and ecosystem tool proofs

High-value local proofs produced 50 successful outputs plus one intentional DLQ record for Firehose custom HTTP through LocalStack, Pub/Sub push envelopes, an Azure Event Hubs Kafka emulator, Elasticsearch, Qdrant, generic webhooks, and an ActiveMQ STOMP/JMS bridge. A Spark/Delta Lake path wrote and read 50 records with one DLQ record.

LocalStack timed out after the Firehose send despite downstream accounting. Delta Lake evidence must not be relabeled as Apache Iceberg. Elasticsearch must not be relabeled as OpenSearch, and Qdrant must not be relabeled as Milvus. Emulator and local-container evidence is not managed-cloud certification.
