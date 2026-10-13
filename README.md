# dbos-langgraph

Unofficial durable execution integration for native LangGraph, backed by DBOS.
Distribution name: `dbos-langgraph`. Import name: `dbos_langgraph`.

**Status: alpha, not production-ready.** Production quality is the objective, not
an assertion about this initial extraction. See [production readiness](docs/production-readiness.md).
This independent project has no runtime dependency on the original prototype.

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
A complete runnable example is [examples/basic.py](examples/basic.py).

## Execution contract

Native Pregel retains planning, routing, reducers, and joins. The graph runs in a
DBOS workflow with no LangGraph checkpointer. Each node invocation runs in an isolated
child workflow. Default `execute_in="step"` wraps its body in one durable step.
`execute_in="workflow"` supports explicit DBOS multistep orchestration inside that
child; all nondeterministic operations must be in steps.

Routing/reducers and the order of durable orchestration must be deterministic.
Pending histories require compatible code and stable registered names. A version
string alone does not enforce compatibility. SDK recovery rules, executor IDs,
and application versions must be managed by the application.

Committed results are reused; unfinished operations may run again. External writes
need idempotency or appropriate transactions. Exactly-once external effects are not promised.

## Supported scope

Explicit async, single-state-argument nodes returning dict updates; native reducers;
fixed cycles and joins; tested simple Send fan-out; isolated multistep nodes.
Sync nodes, config/runtime injection, unwrapped nodes, and invalid registration are rejected.
This facade is not a drop-in CompiledStateGraph replacement.

HITL, streaming, Functional API, runtime/store/context injection, general subgraphs,
dynamic graph changes, full cancellation/retry policy translation, and upgrades are
not supported or validated. Do not use them under assumptions of durable compatibility.
Tests use SIGKILL and actual node-body audit counts; probes live only under tests/support.

## Project provenance

Extracted from `../durable-agent-graph-prototype`; the prototype remains intact.
The plugin is unofficial and has not been published. English is the repository language.
See [architecture](docs/architecture.md), [contributing](CONTRIBUTING.md), and
[validation](docs/validation.md).
