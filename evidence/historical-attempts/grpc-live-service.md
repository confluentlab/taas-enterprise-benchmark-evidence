# gRPC live-service attempt

| Field | Value |
|---|---|
| Objective | Execute unary and streaming transformations through a running gRPC service. |
| Environment | Preserved local full-runtime revalidation. |
| Completion criterion | Live unary and stream requests return transformed output with complete accounting. |
| Observed outcome | Both live operations returned `UNIMPLEMENTED`. |
| Classification | `PRESERVED_FAILURE` |
| Root cause | The live endpoint did not expose the invoked methods. The preserved public evidence does not establish whether service registration, version skew, or deployment configuration caused it. |
| Remediation status | Requires endpoint/service registration verification and a new live rerun. |
| Superseded | No. In-process gRPC contract verification passes, but that is a different execution boundary. |
