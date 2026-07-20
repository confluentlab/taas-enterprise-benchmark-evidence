# Runtime contract output parity

**Status: `CONTRACT_VERIFIED`.**

A fixed mapping and valid/invalid fixture were executed through five in-process contract modes: embedded Java SDK, HTTP single, HTTP batch, gRPC batch, and gRPC stream.

## Fixture identity

| Item | Value |
|---|---|
| Mapping ID / version | `orders-parity` / `v2026.07.03` |
| Artifact identity | `sha256:runtime-output-parity` |
| Mapping DSL SHA-256 | `1de77f0bd2474bf3bc64f16d481240345e4dac94a01c3e1cb856fd786f9d77c4` |
| Valid input SHA-256 | `4ca6e09eb1f41c32388b107866f9a531d1e9ba6145e59c89985fc2c596ca71f6` |
| Expected valid output SHA-256 | `6afb9b453bb3f66f696621dec4de4bf23b57b4de1e16490691062d450c1d6a58` |
| Invalid input SHA-256 | `8125f6a46d19964506ad949a83e4a8fce44cc0059ba1aec83ce41d79c4a49aab` |
| Expected invalid result | `DLQ`; `INVALID_PAYLOAD`; stage `INPUT_PARSE`; SHA-256 of empty output |

The test trims fixture text, encodes it as UTF-8, executes the compiled mapping with JSON-string output, and hashes the exact returned output bytes. The expected output fixture is compact JSON with stable field order. No semantic JSON normalization is applied before hashing.

Every valid mode produced the expected SHA-256. Every invalid fixture produced the expected canonical error and empty-output hash. The full expected-versus-actual record is in [parity-matrix.csv](parity-matrix.csv).

## Version scope

The inspected source revision used Flowplane `1.0.0-SNAPSHOT`, Java 17 compilation target, Spring Boot 3.5.16, Jackson 2.21.5, gRPC 1.82.2, and Protobuf 3.25.5.

## Boundary

This proves deterministic behavior for the preserved mapping and two fixtures at the in-process contract-test boundary. It does not prove equivalence for every mapping, schema, error, or version. It is not a live gRPC deployment proof. The separate live gRPC attempt is retained as a [`PRESERVED_FAILURE`](../historical-attempts/grpc-live-service.md).

See [output hashes](output-hashes.csv), [DLQ results](dlq-results.csv), [protocol matrix](protocol-matrix.csv), [parity matrix](parity-matrix.csv), and the [raw report](parity-output-report.json).
