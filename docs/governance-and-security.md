# Governance and security

## Implemented governance controls

- Versioned drafts, optimistic concurrency, autosave, validation, simulation, and immutable publication artifacts.
- Approval, QA, PII, schema, replay, and deployment gates.
- Staged rollout, deployment state tracking, drift visibility, rollback, and audit export.
- Tenant-aware repositories and permission-aware operations.

## Implemented security boundaries

- JWT/OIDC authentication, RBAC-aware APIs, session revocation, secure cookie and CSRF controls.
- Runtime secrets returned once and stored as hashes; short-lived runtime access tokens.
- Encryption for sensitive persistence and cache values.
- Raw snippets and redrive payload retention disabled by default.
- Customer production payloads execute inside customer-owned runtimes.

These statements describe code paths and tests inspected at revision `10a26df4d7ed6a41f8076a5d7280d73db543c13a`. They are not a SOC 2, ISO 27001, penetration-test, or vendor certification claim. Deployment configuration, key management, identity-provider policy, infrastructure, and operator practice remain part of the security boundary.
