# Changelog

## 0.1.0a2 (unreleased)

- Validate pauses before reserving immutable approval delivery histories.
- Isolate resume messages by interrupt ID and ignore malformed messages.
- Bound approval receives and stop waiting roots after stored cancellation.
- Reuse the registered compiled graph with isolated in-memory checkpoint state.
- Change the opt-in interrupt protocol; earlier pending PR histories require their original code.

- Add opt-in native interrupt/resume for one sequential human approval in a step node.
- Persist approval delivery through a separate idempotent DBOS control workflow.
- Recover paused and resumed nodes without repeating committed model/tool results.
- Return polling handles for interrupt-enabled invocations to observe stored cancellation.

- Run synchronous step nodes in worker threads and reject unsupported preemption.
- Persist supported invocation config and inject isolated snapshots into nodes.

- Support standard LangChain messages in state and canonical replay identities.
- Assign missing message IDs before durable storage and test MessagesState recovery.

- Shield sibling-history cancellation against repeated task cancellation.
- Fix PostgreSQL service health-check quoting in GitHub Actions.

- Register existing StateGraph builders through node execution metadata.
- Reconstruct JSON runtime context in durable nodes and include it in replay checks.

- Discover and reconcile unfinished cancellation requests for confirmed-dead executors.
- Add a cancellation coordinator example and StateGraph migration guide.

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
