# dbos-langgraph

Unofficial durable execution integration for native LangGraph, backed by DBOS.
Distribution name: `dbos-langgraph`. Import name: `dbos_langgraph`.

**Status: alpha.** See [production requirements](docs/production-readiness.md)
for the remaining recovery and deployment work.

## Development

```sh
uv venv --python 3.12
uv pip install -e '.[dev]'
source .venv/bin/activate
ruff check .
ruff format --check .
mypy
pytest -q
python -m build
```

Recovery tests default to separate temporary SQLite databases. To run them against
PostgreSQL, use a dedicated test database:

```sh
docker compose up -d --wait
export GRAPH_TEST_DATABASE_URL='postgresql://test:test@localhost:55433/dbos_langgraph_test'
pytest -q
```

Use `uv sync --extra dev --frozen` for the locked environment. Dependencies are
pinned to the tested DBOS 3.2.0 and LangGraph 1.2.15 combination. Broader compatibility
requires recovery tests; it is not assumed. Python 3.12 is locally tested; CI is
configured for 3.12/3.13 with both SQLite and PostgreSQL, but remote CI has not run.

## API

```python
from dbos_langgraph import DBOSLangGraphPlugin
from langgraph.graph import StateGraph, START, END


async def answer(state):
    return {"answer": "hello"}


plugin = DBOSLangGraphPlugin("my_agent_v1")
builder = StateGraph(dict)
plugin.add_node(builder, "answer", answer)
builder.add_edge(START, "answer")
builder.add_edge("answer", END)
app = plugin.compile(builder)
```

Register every graph and node once per process before `DBOS.launch()`. After
configuring and launching DBOS, call `await app.start(state)` to obtain a workflow
handle, or `await app.ainvoke(state)` to invoke directly. Set a stable workflow ID
with DBOS `SetWorkflowID`; retrieve after restart with `DBOS.retrieve_workflow_async`.
A workflow ID identifies one request. DBOS keeps its first input: reusing that ID
with different state returns the original result without rejecting the changed
input. Use a new ID for a new request, or validate request identity in your service.
The node replay guard does not validate duplicate root requests.
A complete runnable example is [examples/basic.py](examples/basic.py).

For an existing graph, see [migration](docs/migration.md).

## Cancellation

Start cancellation outside a DBOS workflow, using a different ID for the control
request:

```python
from dbos import SetWorkflowID

with SetWorkflowID("cancel-request-123"):
    request = await app.request_cancel("graph-run-123")
await request.get_result()
```

The request is a separate durable workflow that recursively cancels an existing
root of this graph. Its result means the SDK cancellation cascade returned; it
does not mean node cleanup or external effects finished. If its worker dies,
resume the cancellation request and await its result before recovering that
executor's graph histories. Use `app.reconcile_cancellations(dead_executor_ids)`
to discover and finish requests assigned to confirmed-dead executors. See [recovery operations](docs/recovery-operations.md).

## Execution contract

Native Pregel retains planning, routing, reducers, and joins. The graph runs in a
DBOS workflow with no LangGraph checkpointer. Each node invocation runs in an isolated
child workflow. Default `execute_in="step"` wraps its body in one durable step.
`execute_in="workflow"` supports explicit DBOS multistep orchestration inside that
child; all nondeterministic operations must be in steps.

Existing builders can use metadata={"execute_in": "step"} on each plain async
node and app = plugin.register_graph(builder). The builder is copied before wrapping.
Pass JSON context through app.start(state, context=...) or app.ainvoke(...).
Nodes may accept runtime and read runtime.context. See
[the context example](examples/runtime_context.py).

Node inputs and updates must be JSON-compatible values with string object keys.
Recovered results carry an input fingerprint; a changed invocation raises
`ReplayMismatchError`. See [replay and code changes](docs/replay.md) for the exact
check and its limits.

Routing/reducers and the order of durable orchestration must be deterministic.
Pending histories require compatible code and stable registered names. A version
string alone does not enforce compatibility. SDK recovery rules, executor IDs,
and application versions must be managed by the application.

Committed results are reused; unfinished operations may run again. External writes
need idempotency or appropriate transactions. Exactly-once external effects are not promised.

See [node failures and cancellation](docs/failures.md) for leaf-node retry,
timeout, and cancellation options.

Recovery requires verified-dead executors. A fenced database result does not stop
an old body from making external writes. See [recovery operations](docs/recovery-operations.md).

## Supported scope

Explicit async, single-state-argument nodes returning dict updates; native reducers;
fixed cycles and joins; tested simple Send fan-out; isolated multistep nodes.
Sync nodes, config/runtime injection, unwrapped nodes, and invalid registration are rejected.
This facade is not a drop-in CompiledStateGraph replacement.

HITL, streaming, Functional API, config injection and Runtime fields other than context, general subgraphs,
dynamic graph changes, full cancellation/retry policy translation, and upgrades are
not supported or validated. Do not use them under assumptions of durable compatibility.
Tests use SIGKILL and actual node-body audit counts; probes live only under tests/support.

## Documentation

- [Execution model](docs/architecture.md)
- [Replay and code changes](docs/replay.md)
- [Node failures](docs/failures.md)
- [Recovery operations](docs/recovery-operations.md)
- [Test coverage](docs/validation.md)
- [Contributing](CONTRIBUTING.md)
