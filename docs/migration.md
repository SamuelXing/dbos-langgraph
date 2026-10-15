# Moving an existing graph to DBOS

The current migration path supports explicit StateGraph builders with plain sync or async nodes,
JSON and message state, and dict updates. Edges, conditional routing, reducers, and native
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

register_graph wraps plain functions on a copy of an uncompiled builder.
It preserves native node schemas, destinations, and metadata and leaves the
original builder unchanged. Each node must explicitly choose step or workflow
execution. Optional step_options metadata accepts StepOptions. LangGraph
retry/cache/timeout policies and error handlers are rejected; configure retries
and timeouts through DBOS. Runnable compositions remain unsupported. Sync nodes use step execution and run
in worker threads; workflow-mode nodes must be async. The extraction of callable nodes uses the pinned LangGraph internal
wrapper; dependency upgrades need registration and recovery tests.

Nodes accept state alone, with optional parameters named config and runtime,
including keyword-only parameters. Runtime.context is reconstructed from the JSON
context supplied to start or ainvoke. Use a dict or TypedDict context schema;
coercion into dataclass or Pydantic context objects is not supported yet.
Every node gets its own copy, so context
mutation does not communicate state updates between nodes. Other Runtime fields remain unsupported. Use dict updates and reducers for graph
state. The manual plugin.add_node and plugin.compile path remains available.

Choose a stable graph name and retain compatible routing, reducer behavior, and
durable-call order for pending histories. See [replay compatibility](replay.md).

## Adapt state and external calls

State accepts JSON values and standard LangChain message objects. MessagesState
and add_messages can retain HumanMessage, AIMessage, ToolMessage, and the other
standard message classes supported by LangChain's message conversion utilities.
Message content, IDs, tool calls, tool-call IDs, artifacts, usage data, and metadata
are included in replay identity. Custom message subclasses and non-JSON message
metadata are rejected. Dataclasses, arbitrary Pydantic state objects, and tuple
message shorthand remain unsupported.

Use concrete message instances in root input and node updates. The adapter assigns
missing root message IDs before starting the workflow and assigns missing update
IDs from the child workflow ID and field path before storing its result. Explicit
IDs remain unchanged, preserving add_messages replacement and RemoveMessage
behavior. Dict/tuple message shorthand that causes a reducer to generate IDs is
not covered; convert it into message instances first.

The DBOS default Python serializer stores message objects. This is not a portable
cross-language payload format. Workers need compatible LangChain classes; treat
the system database as trusted input for the Python serializer. JSON-only node
fingerprints retain their previous format. See the runnable
[messages agent](../examples/messages_agent.py) for a credential-free model/tool loop.

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
general interrupt/resume, streaming, subgraphs, checkpoint
history inspection, or time travel. Applications using those features need to keep
their existing runtime until equivalent behavior has implementation and recovery
tests. Prebuilt agents are not accepted directly; this API requires explicit node
registration in a StateGraph builder. A runnable LangChain prompt example is
[runtime_context.py](../examples/runtime_context.py); it needs no model credentials.

Start with one isolated graph and new workflow IDs. Test its reducers and routing
against the existing implementation, then inject process crashes around each
external call before moving traffic. Keep the existing checkpoint histories
available to the runtime that created them.


## Invocation config

start and ainvoke accept a second config argument. Supported fields are JSON
configurable values, tags (a list of strings), JSON metadata, run_name, and a positive
integer recursion_limit. Supplying conflicting recursion limits through config and
the keyword argument raises ValueError. The default remains 1000.

Nodes that accept config receive an isolated snapshot of those user fields, with
empty configurable/metadata/tags defaults and the effective recursion limit.
Config affects replay identity and is retained in root and child inputs. Live
callbacks, max_concurrency, internal configurable keys, and other config fields are
rejected before starting a workflow. Native LangGraph callbacks and execution
internals are not the persisted config contract. Mutation inside a node does not
update configuration for other nodes. Use state updates for that purpose.

A configurable thread_id is ordinary invocation data; it does not select a DBOS
workflow ID, resume a thread, or create checkpoint storage. Keep SetWorkflowID for
request identity. A node must explicitly accept config to use the supported
snapshot; implicit access to native execution internals is not guaranteed on replay.

Sync nodes can use ordinary blocking libraries without blocking Pregel's event
loop. StepOptions retries apply, but preemptible and timeout_seconds are rejected
for sync nodes. Cancellation can stop waiting for a sync node while its thread
continues; it cannot undo external effects. See the runnable
[sync config example](../examples/sync_config.py).


## A single human approval

A step node may retain its native `interrupt(payload)` call when the plugin is
registered with `interrupts=True`. Start the graph with `app.start`, observe the
pause through `app.get_interrupt`, and submit its response through `app.resume`.
Do not pass a native `Command` to the facade. Keep external effects outside the
approval node because that node restarts from its beginning. Only one sequential
pause per invocation is supported. See [human approval](interrupts.md) for the
response and recovery contract.
