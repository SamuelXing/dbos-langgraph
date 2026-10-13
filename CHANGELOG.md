# Changelog

## 0.1.0a2 (unreleased)

- Add DurableGraph.request_cancel with a separate durable cancellation history.
- Verify recovery after interrupted recursive cancellation and leaf cleanup.

- Verify committed node-result reuse after a lost COMMIT acknowledgment, both with
  a live worker and with a crash before confirmation.

- Test result commitment through a bounded connection outage, both with a live
  worker and with a crash before connectivity returns.

- Test reconnecting after termination of a worker's tagged PostgreSQL sessions.
- Test stale-invocation result fencing and document the dead-executor prerequisite.
- Compare committed body counts from their first observed commit in repeated-crash tests.

- Cancel interrupted child histories when Pregel cancels a sibling coroutine.
- Verify that backoff crashes reset unfinished-step attempt budgets.
- Remove development-session notes from the documentation.

- Add StepOptions for leaf retry attempts, backoff, timeout, and preemption.
- Test ordinary failures, successful/exhausted retries, timeouts, and recursive
  cancellation through the SDK.

- Test recovery through successive loop boundaries and competing recovery requests
  from distinct PostgreSQL workers.

- Exercise three consecutive worker crashes under seeded completion delays for
  cycles and Send fan-out.

- Check recovered node inputs before applying their state updates.
- Raise ReplayMismatchError when routing changes select a different stored input.
- Require JSON-compatible node inputs and updates.
- Add a crash/recovery test for changed Send order.

The stored node-result format changed. Recover histories created by 0.1.0a1 with
0.1.0a1; there is no migration to the new format.

## 0.1.0a1 — unreleased

- Add the native LangGraph/DBOS adapter.
- Isolate node histories with child workflows and retain native Pregel semantics.
- Validate registration names, builder ownership, node signatures, and invocation limits.
- Add typed package metadata, reproducible dependencies, crash fixtures, and CI configuration.
