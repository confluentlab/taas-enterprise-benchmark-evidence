# How it works

## Author and simulate

Mappings are versioned drafts protected by optimistic concurrency and dirty-state/autosave handling. Compilation validates field references, transformations, policies, and schema expectations. Single-record and batch simulation expose transformed output, field-level failures, policy decisions, and timing before publication.

## Review and publish

Approval, QA, PII, schema, replay, and deployment gates can block publication. A successful publish creates an immutable artifact with version and integrity identity. Audit events record lifecycle decisions.

## Assign and execute

Customer runtimes authenticate using one-time credentials exchanged for short-lived tokens. Runtime instances poll for assignments, cache verified artifacts, execute locally, and report capabilities, deployment state, metrics, lag, canonical failures, replay outcomes, and schema observations.

## Observe and recover

Operators use live telemetry and failure drilldowns to detect drift or unhealthy rollout behavior. Staged deployment and rollback keep runtime changes reversible. Raw payload retention is intentionally minimized; snippets and redrive payload retention are off by default.
