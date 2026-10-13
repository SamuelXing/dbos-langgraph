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
