# Test coverage

Tests use the installed DBOS and LangGraph APIs with real subprocesses and SIGKILL.
The local environment is Python 3.12.12, DBOS 3.2.0, LangGraph 1.2.15, and PostgreSQL
14.20. SQLite tests use separate temporary databases. PostgreSQL tests use a
dedicated database and isolate application versions and recovery ownership per case.

## Recovery checks

- Cycles, including a crash after 100 rounds, compared with a pure reference executor.
- Parallel joins and Send fan-out with audit counts for actual node-body executions.
- Changed Send order rejected by ReplayMismatchError.
- Multistep nodes replayed with perturbed scheduling inside isolated child histories.
- Three consecutive crashes at one unfinished node under two seeded delay families.
- Crashes during three successive loop rounds.
- Competing recovery requests from two distinct PostgreSQL workers for a dead
  worker's history. The test synchronizes requests before releasing unfinished work.

Audit markers are written after a child result is committed. A result may commit
before its marker is written, so an absent marker does not prove unfinished work.
Assertions freeze each body's execution count when its commit is first observed
and require that count to remain unchanged on later recoveries. A body may already
have repeated before that observation. Separate assertions check known committed
and deliberately unfinished nodes. Seeded delays vary body completion timing,
not child registration order.

The competing-worker harness calls DBOS._recover_pending_workflows to exercise the
SDK recovery-service path. The installed adapter does not call that internal API.
Both workers must retrieve the expected result without unexpected body executions.

## Duplicate requests

Two cases start eight concurrent requests in one process while the original node
is held, then reuse the ID through start and ainvoke after completion. Identical
and changed inputs both return the original result; the node body runs once.
These tests characterize DBOS first-input behavior, not input-conflict rejection.
Two more cases hold an original node while two separate executors start its root
ID with identical or changed inputs. Both callers receive the original result,
and audit files across all processes contain one body execution. These cases
establish reuse of an existing pending history; they do not race the first
creation of an ID or crash its owner during duplicate starts.

## PostgreSQL fault checks

The connection test terminates only sessions tagged for that worker, then checks
reconnection and one body execution per node. The stale-invocation test holds an
original body while recovery claims its history, lets that body return an altered
update, and verifies that the expected graph result survives. Recovered jobs may
run on either process, so the altered update belongs only to the first invocation.
Two proxy tests block a worker's database connection path for three seconds after
a node body returns. Direct database inspection checks that the root remains
PENDING and the step output is absent. Restoring the connection commits the result
without another body execution; killing the worker during the outage causes that
unfinished body to execute again on recovery. The committed sibling is reused.

Two acknowledgment tests withhold a known COMMIT response until another database
session sees the stored step result. Dropping that response or killing the worker
before it receives confirmation must leave every node body executed once after
the graph completes.

See [recovery operations](recovery-operations.md) for deployment prerequisites.

## Failure checks

The suite covers ordinary node errors, successful and exhausted retries, timeouts,
and recursive cancellation of a preemptible async leaf. Reading a terminal handle
again must return the same result or error without rerunning the node. Validation
tests cover registration, invocation limits, the JSON state schema, and StepOptions.

Two backoff crash cases verify that an unfinished step starts a fresh retry budget
on recovery. A parallel failure case checks interrupted-child cancellation and
restarts the worker to verify the child is not revived.

## Running the checks

```sh
uv sync --extra dev --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```

Set GRAPH_TEST_DATABASE_URL to a dedicated PostgreSQL database to run that backend.
The competing-worker and PostgreSQL fault cases are skipped on SQLite. Process
logs are retained in the pytest temporary directory. The wheel contains the library and its type marker;
test fixtures are excluded.

## Limits

Hosted CI, Python 3.13, Docker Compose, network partitions, rolling upgrades, and
prolonged database outages have not been verified locally. Identical-input
task identity is not established by the input fingerprint guard. These tests do not
promise exactly-once external side effects.

## Latest local run

| Check | Result |
| --- | --- |
| PostgreSQL suite | 48 passed, 165.88 seconds |
| SQLite suite | 41 passed, 7 PostgreSQL-only cases skipped, 133.29 seconds |
| Ruff lint and formatting | Passed |
| Strict mypy | Passed |
| Source distribution and wheel build | Passed |
| Installed wheel import outside checkout | Passed |

The acknowledgment-loss cases also passed a focused run requiring proof that the
relay held the server response: 2 passed in 7.96 seconds.

All four duplicate-request cases passed on SQLite (4 passed in 21.55 seconds)
and PostgreSQL (4 passed in 20.05 seconds). The full-suite results above predate
these four cases.
