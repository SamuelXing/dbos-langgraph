# Validation

Extraction acceptance covers SIGKILL at committed/uncommitted node boundaries for
short cycles, 100-cycle histories, dynamic Send, and isolated multistep nodes.
Final state is checked against an independent pure-executor reference. Audit records
verify committed bodies are reused and unfinished bodies run again.

Workers register the complete graph catalog before launch. Each test has an isolated
executor/application version reused on restart to avoid cross-worker audit contamination.
These isolated tests do not establish simultaneous production-worker recovery guarantees.

The original prototype recorded successful SQLite/PostgreSQL cases before extraction.
Independent-project execution results will be appended below. CI configuration is not
itself evidence that hosted jobs passed. No existing SDK checkout is a runtime dependency.

## Local extraction results

Verified on macOS with Python 3.12.12, DBOS 3.2.0, and LangGraph 1.2.15:

- SQLite acceptance suite: 6 passed in 40.72 seconds (four crash/recovery cases
  and the two initial registration checks).
- PostgreSQL 14.20 acceptance suite: 6 passed in 49.13 seconds, using a dedicated
  local test database through the Unix socket.
- Expanded input-validation suite: 11 passed, including invalid names and recursion
  limits rejected before workflow execution.
- Ruff lint/format checks and strict mypy succeeded.
- Source distribution and wheel built; wheel installed into a fresh environment
  and imported from outside the project checkout. Wheel includes only the library
  and package metadata, not the test fixtures.
- Basic example completed with SQLite and returned `{'answer': 'Hello, World'}`.

The first test invocation failed because the explicitly chosen test-artifact parent
folder did not exist; creating it allowed both acceptance suites to complete.
Docker Compose, hosted CI, Python 3.13, randomized crash campaigns, and production
rolling deployments were not exercised locally.

## Replay input guard (0.1.0a2)

The final suite passed 21 tests on each backend: SQLite in 47.31 seconds and
PostgreSQL in 47.77 seconds. Five cases use real process crashes, including a case
that changes Send order on restart and expects ReplayMismatchError. Sixteen tests
cover registration, invocation limits, and the JSON fingerprint schema.

Ruff, strict mypy, distribution builds, and importing the installed 0.1.0a2 wheel
from outside the checkout passed.

During development, PostgreSQL exposed a race in the test observations: a durable
child result may commit before the parent writes its marker. The assertions now
require marked results to execute once, the deliberately blocked body to execute
twice, and unmarked bodies to execute at most once more after the crash. The marker
is an observation after commit, not an atomic database-commit record.

This milestone adds input mismatch detection. It does not establish stable identity
for identical-input tasks or cover randomized faults, concurrent recovery, retry,
cancellation, or database-disconnect behavior.

## Repeated recovery with seeded delays

The full suite passed 25 tests on SQLite in 83.20 seconds and PostgreSQL in
81.68 seconds. Ruff lint/format and strict mypy also passed.

Four new cases cover cycles and Send fan-out with seed families 7 and 19. Each
case kills the worker three times at the same blocked node, then allows a final
recovery to finish. Seeds change between restarts. Delays run inside node bodies,
so they vary completion timing without changing durable child registration order.

Assertions check that marked committed node bodies run once, the blocked body
runs four times, and the recovered state matches the expected reducers and routing.
Each attempt writes a separate log under pytest's temporary directory.

These cases do not test arbitrary task registration reorderings, changing crash
boundaries, database faults, or recovery by two workers at once. There is no change
to the installed adapter or its stored history format in this milestone.

## Successive boundaries and competing workers

The full suite passed 27 tests on PostgreSQL in 102.41 seconds. SQLite passed
26 tests in 98.66 seconds and skipped the PostgreSQL-only competing-worker test.
Ruff lint/format, strict mypy, and diff whitespace checks passed.

The successive-boundary case kills the graph during the slow node in each of the
first three loop rounds. It checks that previously observed committed bodies are
reused, each blocked body executes twice, and four rounds reconstruct the expected
state.

The competing-worker case first kills the original worker. Two processes with
distinct executor IDs then reach a barrier before requesting recovery for that
worker's history. The test keeps the unfinished node blocked until both recovery
requests return. Both workers must retrieve the expected graph result. Body counts
must show one restart of the interrupted node, no repeats of observed committed
nodes, and at most one additional execution of other interrupted bodies.

This test invokes DBOS._recover_pending_workflows in the harness to exercise the
SDK recovery-service path. The adapter does not depend on that internal API.
Application versions are isolated per test. The result applies to competing
requests for a dead worker's history, not recovery of a worker that is still alive,
network partitions, rolling deployment, or a general exactly-once side-effect claim.
No library code or stored history format changed in this milestone.

## Leaf failures and step options

The full suite passed 39 tests on PostgreSQL in 115.70 seconds. SQLite passed
38 tests in 120.09 seconds, with the competing-worker case skipped. Ruff lint,
format checks, strict mypy, distribution builds, and importing StepOptions from
the installed wheel outside the checkout passed.

Five new execution cases use the real adapter and SDK: a transient failure succeeds
on attempt three; a plain error executes once; exhausted retries execute three
times; a timeout exits the async body; recursive cancellation stops a preemptible
leaf and leaves the root CANCELLED. Reading each terminal handle again returns the
same result or error without extra body attempts. Seven validation tests reject
invalid options and options supplied to workflow-mode nodes.

This establishes the leaf policies described in docs/failures.md. It does not
cover process crashes during backoff, sibling failure in a parallel superstep,
resume after cancellation, blocking tools, or cancellation-suppressing code.
The default policy retains the previous one-attempt, non-preemptible behavior.
There is no new retry loop or history-format change in the adapter.
