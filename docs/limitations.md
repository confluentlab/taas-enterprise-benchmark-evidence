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

- Contract verification is separate from live deployment proof. The older gRPC service attempt returned `UNIMPLEMENTED`; newer sidecar builds completed live gRPC batch and bidirectional-streaming baseline proofs plus separate 10,000-record/five-minute stability soaks.
- Local Docker, LocalStack, Azurite, Event Hubs emulator, and Pub/Sub emulator evidence does not cover managed-cloud networking, identity, quotas, service behavior, failover, or operations.
- The older 60,000-record HTTP single attempt remains `INCOMPLETE`, but it is superseded as a current-interoperability indicator by newer 110-record and 10,000-record successful local runs. The newer soak does not retroactively make the older 60,000-record workload pass.
- Tool and sidecar evidence qualifies only the documented local interoperability path. It does not make an HTTP/sidecar integration a first-class native wrapper.
- The GCP Pub/Sub emulator does not provide Eventarc. The local bridge converts only the incoming Pub/Sub push envelope to a binary CloudEvent envelope and is explicitly part of that proof boundary.
- Five-minute stability evidence applies only to Embedded Spring/JVM, HTTP single, HTTP batch, gRPC batch, and gRPC streaming. It is not a general stability claim for every tool integration.

## Non-claims

- No vendor certification.
- No independent security audit or compliance attestation.
- No universal runtime-equivalence claim beyond the fixed contract fixture.
- No universal throughput or latency guarantee.
- No claim that implementation presence alone equals execution verification.

See [Evidence classification](evidence-classification.md) and [Historical attempts](../evidence/historical-attempts/README.md).
