# Moving an existing graph to DBOS

The current migration path supports explicit StateGraph builders with async nodes,
JSON state, and dict updates. Edges, conditional routing, reducers, and native
Pregel scheduling remain in LangGraph. DBOS replaces checkpoint-based recovery for
new invocations. Existing LangGraph checkpoints cannot be imported.

## Register nodes through the plugin

An existing graph might register and compile its nodes like this:

```python
builder.add_node("agent", agent_node)
builder.add_node("tools", tools_node)
# Add edges and conditional routing.
app = builder.compile(checkpointer=checkpointer)
```

Keep ordinary node registration and declare DBOS execution in metadata:

```python
from dbos_langgraph import DBOSLangGraphPlugin

builder.add_node("agent", agent_node, metadata={"execute_in": "step"})
builder.add_node("tools", tools_node, metadata={"execute_in": "step"})
# Keep the edges, conditional routing, and reducers.
app = DBOSLangGraphPlugin("agent_v1").register_graph(builder)
```

register_graph wraps plain async functions on a copy of an uncompiled builder.
It preserves native node schemas, destinations, and metadata and leaves the
original builder unchanged. Each node must explicitly choose step or workflow
execution. Optional step_options metadata accepts StepOptions. LangGraph
retry/cache/timeout policies and error handlers are rejected; configure retries
and timeouts through DBOS. Runnable compositions and synchronous nodes are not
supported yet. The extraction of callable nodes uses the pinned LangGraph internal
wrapper; dependency upgrades need registration and recovery tests.

Nodes accept state alone or state plus a parameter named runtime, including a
keyword-only runtime parameter. Runtime.context is reconstructed from the JSON
context supplied to start or ainvoke. Use a dict or TypedDict context schema;
coercion into dataclass or Pydantic context objects is not supported yet.
Every node gets its own copy, so context
mutation does not communicate state updates between nodes. Config injection and
other Runtime fields remain unsupported. Use dict updates and reducers for graph
state. The manual plugin.add_node and plugin.compile path remains available.

Choose a stable graph name and retain compatible routing, reducer behavior, and
durable-call order for pending histories. See [replay compatibility](replay.md).

## Adapt state and external calls

State must contain JSON values: null, strings, booleans, finite numbers, lists,
and objects with string keys. LangChain BaseMessage instances, dataclasses, custom
objects, and tuples are not accepted. Serialize message history into JSON records
and reconstruct model inputs inside the node. Ensure tool results also meet the
state contract. Preserve message IDs and tool-call fields needed by your agent;
reducing a message to its text alone can lose routing information.

The default node body runs as one DBOS step. Model and tool libraries can remain
inside that body, but an unfinished body may run again after a crash. A node that
calls several tools or performs several writes should instead use
execute_in="workflow" and wrap each nondeterministic operation in a named DBOS
step. Put routing and reducer computation outside those effects. External writes
still need idempotency; a durable result does not atomically commit a remote write.

## Start new invocations

Register the graph once before DBOS.launch(). Configure a dedicated system database
and launch the SDK as shown in [the basic example](../examples/basic.py). Then:

```python
from dbos import SetWorkflowID

with SetWorkflowID("request-123"):
    handle = await app.start(initial_state, context={"user_id": "alice"})
result = await handle.get_result()
```

A LangGraph thread_id does not automatically become a DBOS workflow ID. Choose an
ID for each logical graph invocation. Retrying that ID retrieves its original
invocation; it does not append a new conversational turn. For a multi-turn chat,
keep conversation state in your application and start a new graph invocation for
each turn, with that state as input. The adapter currently provides no thread store.

Use DBOS.retrieve_workflow_async(id) to obtain a stored result after restart.
Retrieval alone does not recover a dead executor. See [recovery operations](recovery-operations.md)
for executor ownership, cancellation ordering, and recovery prerequisites.

## Features that need another migration path

The adapter is not a CompiledStateGraph replacement. It does not implement
interrupt/resume, streaming, subgraphs, config injection, checkpoint
history inspection, or time travel. Applications using those features need to keep
their existing runtime until equivalent behavior has implementation and recovery
tests. Prebuilt agents are not accepted directly; this API requires explicit node
registration in a StateGraph builder. A runnable LangChain prompt example is
[runtime_context.py](../examples/runtime_context.py); it needs no model credentials.

Start with one isolated graph and new workflow IDs. Test its reducers and routing
against the existing implementation, then inject process crashes around each
external call before moving traffic. Keep the existing checkpoint histories
available to the runtime that created them.
