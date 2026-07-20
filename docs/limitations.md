# Scope and limitations

## Measurement scope

- Performance runs used a developer workstation rather than an isolated performance lab.
- Results apply to the documented synthetic fixtures, source revision, host, JVM, and runtime configuration.
- Core-engine JMH and live runtime measurements have different input and output boundaries and should not be compared as if they measure the same work.
- Large referenced fields and materialized outputs can change allocation materially. The fixed-output scaling fixture does not represent every large-payload mapping.

## Qualification status

- The 1 MiB core benchmark completed with valid measurements and 174 passing tests.
- Nine of twelve cross-run repeatability checks met their thresholds. Three mean-latency spread checks exceeded the 5% limit: 7.763% for success group B, 5.007% for bounded-error group A, and 8.525% for bounded-error group B.
- `MEASURED_NOT_QUALIFIED` is distinct from `QUALIFIED`. Full thresholds and observations are in [Repeatability qualification](repeatability-qualification.md).

## Runtime evidence scope

- Contract verification is separate from a live deployment proof. The gRPC contract fixture passed, while the preserved live service attempt returned `UNIMPLEMENTED`.
- Local Docker, LocalStack, and emulator evidence does not cover managed-cloud networking, identity, quotas, service behavior, or operations.
- The latest HTTP single-record full run completed 59,600 of 60,000 records and is `INCOMPLETE`. HTTP batch completed 60,000 of 60,000 in its preserved local run.
- Several tool and sidecar integrations are `MEASURED`, not native-runtime qualifications.

## Non-claims

- No vendor certification.
- No independent security audit or compliance attestation.
- No universal runtime-equivalence claim beyond the fixed contract fixture.
- No universal throughput or latency guarantee.
- No claim that implementation presence alone equals execution verification.

See [Evidence classification](evidence-classification.md) and [Historical attempts](../evidence/historical-attempts/README.md).
