# Integration proof index

The current local evidence set proves 22 streaming/tool integrations and eight Flowplane runtime/protocol surfaces. Five core surfaces also have separate 10,000-record, five-minute stability runs, and three provider-trigger proofs are preserved under `evidence/trigger-proofs/`.

Start with the complete [proven local integration evidence overview](EVIDENCE-OVERVIEW.md). It lists every successful run, its exact execution boundary, screenshots or native operational evidence, accounting, reproduction entry point, and qualification limits.

## Current result

| Evidence group | Successful bundles | Attempted inputs | Transformed | Intentional DLQ |
|---|---:|---:|---:|---:|
| 22 streaming/tool integrations | 22 | 2,420 | 2,200 | 220 |
| Eight runtime/protocol baselines | 8 | 880 | 800 | 80 |
| Five core-surface stability soaks | 5 | 50,000 | 49,500 | 500 |
| Three provider-trigger proofs | 3 | 330 | 300 | 30 |
| **Total** | **38** | **53,630** | **52,800** | **830** |

All 38 bundles passed their documented gates and per-bundle checksum verification. They report zero duplicates and zero unexplained loss. Final lag or pending work is zero wherever that concept applies.

## Evidence policy

These are local technical-interoperability proofs, not vendor certifications. A green row applies only to its recorded fixture, source/worktree identity, runtime images, host, and execution boundary. Screenshots corroborate the proof; manifests, outputs, hashes, write-boundary audits, counts, logs, metrics, and health observations are primary evidence.

The verifier did not manually insert downstream results. See [Proof boundary: no downstream fixture insertion](EVIDENCE-OVERVIEW.md#proof-boundary-no-downstream-fixture-insertion) and the [reproduction guide](../../reproduction/README.md).

Earlier incomplete and failed runs remain under [historical attempts](../historical-attempts/README.md). They are historical records and must not be described as the latest status after the successful 2026-07-20/21 reruns.
