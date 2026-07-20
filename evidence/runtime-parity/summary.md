# Runtime contract output parity

A fixed mapping (`v2026.07.03`) and valid/invalid fixtures were exercised through five in-process contract modes: embedded Java SDK, HTTP single, HTTP batch, gRPC batch, and gRPC stream.

Every valid mode produced SHA-256 `6afb9b453bb3f66f696621dec4de4bf23b57b4de1e16490691062d450c1d6a58`. Every invalid fixture produced status `DLQ`, code `INVALID_PAYLOAD`, stage `INPUT_PARSE`, and an empty output hash.

This proves deterministic output for the preserved fixture at the contract-test boundary. It does not prove equivalence for all mappings and errors, nor a live gRPC deployment. The latest live gRPC service attempt returned `UNIMPLEMENTED`.

See [output hashes](output-hashes.csv), [DLQ results](dlq-results.csv), [protocol matrix](protocol-matrix.csv), and the [raw report](parity-output-report.json).
