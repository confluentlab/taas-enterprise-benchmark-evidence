# Failure handling

FlowPlane represents execution failures using a canonical envelope: error code, stage, field/path context, policy disposition, runtime/deployment identity, and bounded metadata. A runtime can fail the record, route it to a DLQ, skip a field under policy, or stop a deployment according to its configured contract.

The parity fixture demonstrates a deterministic invalid-input outcome: status `DLQ`, code `INVALID_PAYLOAD`, stage `INPUT_PARSE`, and no transformed output. The 30-minute soak injected 10,800 missing-tenant failures and verified exact accounting:

```text
1,069,201 successful outputs + 10,800 intentional failures = 1,080,001 inputs
```

Replay is capability-aware and gated. Payload retention for redrive is off by default, so a replay workflow must use an explicitly enabled payload source. See [Kafka soak evidence](../evidence/kafka-soak/summary.md) and [Governance and security](governance-and-security.md).
