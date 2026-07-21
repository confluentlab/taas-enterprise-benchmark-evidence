# gRPC status

Live local gRPC batch and bidirectional streaming are now `LIVE_LOCAL_VERIFIED` for the current sidecar build.

- [`TransformBatch` baseline](grpc-batch/runs/20260721T045105Z/summary.md): 100 transformed and 10 intentional DLQ records.
- [`TransformBatch` stability soak](grpc-batch/runs/20260721T054138Z/summary.md): 9,900 transformed and 100 intentional DLQ records over 300.920 seconds.
- [`TransformStream` baseline](grpc-streaming/runs/20260721T045213Z/summary.md): 100 transformed and 10 intentional DLQ records through one long-lived bidirectional stream.
- [`TransformStream` stability soak](grpc-streaming/runs/20260721T054141Z/summary.md): 9,900 transformed and 100 intentional DLQ records over 301.327 seconds.

The verifier wrote only raw Kafka input. The runtime bridge invoked the live gRPC method and transported the actual returned response to transformed/DLQ Kafka topics. Expected output was generated separately and compared afterward.

The older service attempt that returned `UNIMPLEMENTED` remains a valid [historical failure record](../historical-attempts/grpc-live-service.md) for that older binary/configuration. It is not the latest status of the current sidecar build.
