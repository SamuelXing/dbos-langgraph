# Production readiness gates

The first extracted release is alpha. Passing the current subset of tests is necessary
but insufficient for production readiness. Each capability must have a supported API,
recovery tests, failure semantics, and documentation before becoming a release promise.

## Gate 1: durable core

- Independent packaging, reproducible dependency lock, typed API, lint and unit checks.
- SIGKILL recovery for loops, parallel joins, dynamic fan-out, and multistep isolation
  on SQLite and PostgreSQL; repeated and randomized scheduling perturbations.
- Fail closed on unsupported graph features; structured diagnostics for replay mismatch.
- Error/retry/cancellation behavior, duplicate invocations, simultaneous recovery,
  and database disconnects tested explicitly.
- Stable node/task identities and documented SDK registration/version contracts.

Initial extraction establishes packaging and existing crash cases. Randomized testing,
concurrent recovery guarantees, comprehensive database faults, and fail-closed feature
inspection are still incomplete.

## Gate 2: framework integration

- Config/runtime/context serialization with an explicit supported schema.
- interrupt/resume and durable human-input delivery with repeat-crash tests.
- Subgraph semantics and Functional API, including parallel nested orchestration.
- Streaming delivery and deduplication contract; token/events consistency on replay.
- Explicit mapping of timeout, cancellation, and retry policies; prevent double retries.

None of these advanced capabilities is an initial release claim.

## Gate 3: operations and compatibility

- CI recovery matrix across declared SDK/Python/database versions, plus wheel-install checks.
- Application upgrades with pending workflows, rolling deployment, schema/version handling,
  and deprecation policy verified in executable tests.
- Structured logs, trace correlation, history growth metrics, and operational runbooks.
- Comparable benchmarks for recovery latency, history growth, CPU, and retention policies.
- Dependency/update review, disclosure process, licensing review, and release checklist.

CI is configured but has not run remotely. No release or production designation is
allowed until the applicable gates are met and evidence is recorded.

## Suggested implementation order

1. Stabilize core identities, recovery behavior, and unsupported-feature detection.
2. Add runtime context and policy translation with fault-injection coverage.
3. Implement interrupt/resume, then subgraphs, then streaming.
4. Exercise upgrades and concurrent recovery; broaden the supported version matrix.
5. Publish an alpha only after explicit authorization and artifact review.
