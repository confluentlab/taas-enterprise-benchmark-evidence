# Limitations

- FlowPlane is pre-GA and under active development.
- The latest controlled 1 MiB core candidate failed 3 of 12 repeatability gates and is not publication-eligible.
- The latest preserved live gRPC service attempt returned `UNIMPLEMENTED`; only in-process contract parity is demonstrated here.
- The latest full HTTP single-record run processed 59,600 of 60,000 records; it is incomplete. HTTP batch processed 60,000 of 60,000 in its preserved run.
- Several sidecar and ecosystem integrations are measured, not fully passed or vendor-certified.
- Local Docker, LocalStack, and emulator proofs do not validate managed-cloud networking, identity, quotas, service behavior, or operations.
- Benchmark hosts were developer workstations, not isolated production performance labs.
- The scaling fixture grows an unreferenced field and holds output size constant; realistic referenced large values allocate materially more.
- Runtime parity hashes use a fixed fixture and do not prove equivalent behavior for every mapping, schema, error, or version.
- Security controls described here are implementation evidence, not an independent audit or compliance attestation.
