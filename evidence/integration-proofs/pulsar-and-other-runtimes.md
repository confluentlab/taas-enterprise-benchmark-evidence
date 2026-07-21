# Pulsar and ecosystem runtime paths

The latest local evidence is no longer limited to a small Pulsar/Bento subset. Twenty-two streaming and ecosystem tools now have checksum-verified `LIVE_LOCAL_VERIFIED` bundles using the common 100-valid/10-invalid fixture and raw-only verifier boundary.

Apache Pulsar’s narrow proof remains representative: a verifier wrote only the persistent raw topic; an independent Pulsar pipeline consumed it, called the Flowplane HTTP sidecar, and wrote the returned success or error to persistent transformed/DLQ topics. The [Pulsar evidence folder](pulsar/) preserves the pipeline source, UI evidence, write-boundary audit, exact output hashes, counts, runtime logs, environment, and checksums.

Other tools use their native input and output surfaces where practical, with Flowplane embedded directly or invoked through HTTP/sidecar transport. They must not be described as native Flowplane wrappers unless the product contains a dedicated runtime module. See the [complete evidence overview](EVIDENCE-OVERVIEW.md) for the tested boundary of every tool.
