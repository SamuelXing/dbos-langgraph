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
Assertions require marked results to be reused and bound the additional executions
of unmarked bodies. Seeded delays vary body completion timing, not child registration
order.

The competing-worker harness calls DBOS._recover_pending_workflows to exercise the
SDK recovery-service path. The installed adapter does not call that internal API.
Both workers must retrieve the expected result without unexpected body executions.

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
The competing-worker case is skipped on SQLite. Process logs are retained in the
pytest temporary directory. The wheel contains the library and its type marker;
test fixtures are excluded.

## Limits

Hosted CI, Python 3.13, Docker Compose, stale-worker recovery, network partitions,
rolling upgrades, and database outages have not been verified locally. Identical-input
task identity is not established by the input fingerprint guard. These tests do not
promise exactly-once external side effects.

## Latest local run

| Check | Result |
| --- | --- |
| PostgreSQL suite | 42 passed, 131.15 seconds |
| SQLite suite | 41 passed, 1 PostgreSQL-only case skipped, 133.17 seconds |
| Ruff lint and formatting | Passed |
| Strict mypy | Passed |
| Source distribution and wheel build | Passed |
| Installed wheel import outside checkout | Passed |
