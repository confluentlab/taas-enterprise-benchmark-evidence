# gRPC live-service attempt

| Field | Value |
|---|---|
| Objective | Execute unary and streaming transformations through a running gRPC service. |
| Environment | Preserved local full-runtime revalidation. |
| Completion criterion | Live unary and stream requests return transformed output with complete accounting. |
| Observed outcome | Both live operations returned `UNIMPLEMENTED`. |
| Classification | `PRESERVED_FAILURE` |
| Root cause | The live endpoint did not expose the invoked methods. The preserved public evidence does not establish whether service registration, version skew, or deployment configuration caused it. |
| Remediation status | Superseded for current interoperability by newer live sidecar runs; retained to document the earlier binary/configuration. |
| Superseded | Yes. Newer live `TransformBatch` and `TransformStream` sidecar runs passed; this failure remains valid only for the preserved historical attempt. |
