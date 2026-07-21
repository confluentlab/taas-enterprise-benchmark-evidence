# Evidence classification

Flowplane uses one status vocabulary across summaries, manifests, claims, charts, and integration records. A status describes the verification boundary. It does not upgrade implementation presence into execution evidence.

| Status | Meaning |
|---|---|
| `QUALIFIED` | Completed and passed every defined acceptance criterion for that evidence set. |
| `MEASURED` | Completed with valid measurements; no formal qualification was applied. |
| `MEASURED_NOT_QUALIFIED` | Completed with valid measurements, but one or more qualification thresholds were missed. |
| `CONTRACT_VERIFIED` | Behavior was verified at an in-process or protocol contract boundary. |
| `SOURCE_INSPECTED` | A statement is supported by inspection of private implementation source or tests, but no public execution artifact independently verifies the behavior. |
| `LIVE_LOCAL_VERIFIED` | Executed through a live local runtime, broker, service, container, or emulator and met the documented local criterion. |
| `INCOMPLETE` | Started but did not satisfy completion or accounting criteria. |
| `PRESERVED_FAILURE` | A failed attempt is retained with its objective and observed failure. |
| `NOT_TESTED` | Implementation or compatibility exists, but no preserved execution evidence covers the stated scope. |
| `VENDOR_CERTIFIED` | Independently certified by the relevant vendor. Flowplane currently makes no claims with this status. |

## How to read combined evidence

A runtime can have more than one status because the boundaries differ. For example, the gRPC fixture is `CONTRACT_VERIFIED`, an older live-service attempt is `PRESERVED_FAILURE`, and newer sidecar runs are `LIVE_LOCAL_VERIFIED`. Historical status remains attached to its historical artifact.

`MEASURED_NOT_QUALIFIED` means the measured values remain available as engineering evidence. It does not mean the benchmark crashed or produced incorrect output. The missed qualification must be named beside the result.

See [Repeatability qualification](repeatability-qualification.md), [Runtime portability](runtime-portability.md), and the [Evidence manifest](../evidence/manifest.json).
