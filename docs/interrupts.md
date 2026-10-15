# Human approval

Enable `interrupts=True` when registering a graph to use native
`langgraph.types.interrupt()` inside a step node. The current scope is one
sequential interrupt per graph invocation. Both sync and async step nodes are
supported. Interrupts inside workflow-mode nodes, multiple pauses, and parallel
interrupts are unsupported.

```python
from langgraph.types import interrupt


def approval(state):
    return {"approved": interrupt({"action": state["action"]})}


app = DBOSLangGraphPlugin("approval_v1", interrupts=True).register_graph(builder)
```

Register the node with `metadata={"execute_in": "step"}` as usual. See the
[runnable approval example](../examples/human_approval.py). With the development
environment installed:

```sh
DBOS_SYSTEM_DATABASE_URL=sqlite:///approval.sqlite python examples/human_approval.py
```

For PostgreSQL, start `docker compose up -d --wait` and set
`DBOS_SYSTEM_DATABASE_URL=postgresql://test:test@localhost:55433/dbos_langgraph_test`.
Use a separate database for application data and automated tests.

## Observe and answer a pause

```python
with SetWorkflowID("approval-request-123"):
    handle = await app.start(initial_state)

pause = await app.get_interrupt("approval-request-123", timeout_seconds=30)
if pause is not None:
    await app.resume("approval-request-123", pause.id, True)
result = await handle.get_result()
```

`get_interrupt` returns the native Interrupt object, with `id` and `value`, or
None when no pause event is available within the timeout, the graph has continued,
or its stored status is terminal. It validates that the workflow belongs to this
graph. Timeout zero checks without waiting. Pause payloads and responses must be
JSON values; False, None, and empty objects are valid responses.

Call `resume` outside a DBOS workflow. It validates the pending pause and sends a
durable message through a separate control workflow. Its return acknowledges
message delivery, not graph completion. Await the graph handle for the result.
The first response submitted for a workflow/interrupt pair wins. Retrying with a
different response reuses that first request, including after graph completion.
An unknown interrupt ID or a first response to a terminal root raises ValueError.
Applications should authenticate and authorize approval requests at their boundary.

The graph remains PENDING while waiting. After a worker crash, register the same
graph with compatible code, recover its confirmed-dead executor through DBOS, and
retrieve the root with `DBOS.retrieve_workflow_async(workflow_id)`. Reading a pause
or submitting a response does not itself recover the dead executor. See
[recovery operations](recovery-operations.md). A stored response survives a crash
before the interrupted node finishes; it does not require another approval.

## Replay behavior

DBOS stores the interrupted node result, pause event, response message, and resumed
node inputs. On recovery, Pregel rebuilds state from committed child results. An
invocation-local InMemorySaver retains native pending-task information while that
root executes; it is reconstructed after a crash and is never the durable store.
The pause ID derives from the DBOS child workflow ID. Resume values are explicit
child inputs, allowing the resumed node to recover independently of its parent.

The interrupted node starts again from its beginning after approval. Code before
`interrupt()` therefore runs again. Keep model calls and external writes in
separate durable nodes, and keep the approval node free of external effects.
An unfinished resumed step may also repeat after a crash. Committed model/tool
nodes are reused; external effects still require idempotency.

Cancellation marks the root terminal and prevents a later first approval from
running the tool. Interrupt-enabled `start` returns a polling handle so stored
cancellation is observable while the local task is still waiting for its receive
timeout. Cancellation does not guarantee immediate task shutdown or external
cleanup. The usual [cancellation limits](failures.md) still apply.

## Compatibility

Graphs without `interrupts=True` retain their existing durable call order. Enable
this option under a new graph name for new invocations; do not change it on a
pending history. The implementation uses the pinned LangGraph scratchpad contract,
which must be revalidated on dependency upgrades.

This facade does not expose Command-based root invocation, durable LangGraph
checkpoint inspection, `get_state`, `update_state`, time travel, or thread history.
Multiple sequential or parallel pauses need further implementation and recovery
tests before they can be supported.
