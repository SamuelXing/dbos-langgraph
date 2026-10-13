# Node failures and cancellation

Leaf nodes accept `step_options=StepOptions(...)`. These options are passed to
DBOS.step; the adapter does not add a second retry loop.

```python
from dbos_langgraph import StepOptions

plugin.add_node(
    builder,
    "fetch",
    fetch,
    step_options=StepOptions(
        max_attempts=3,
        interval_seconds=0.5,
        backoff_rate=2.0,
        timeout_seconds=30.0,
        preemptible=True,
    ),
)
```

`max_attempts` counts total attempts, including the first call. The default is one
attempt. Retries apply to any ordinary exception raised by the node, including an
invalid returned update. Each attempt receives a fresh copy of its input state.
The timeout applies separately to each attempt. Defaults are no timeout and no
preemption. Interval, backoff, and timeout values must be finite and positive.
There is no exception filter in this adapter API yet.

A node in workflow mode configures its own DBOS steps. Passing StepOptions to that
node is rejected. Do not add a LangGraph retry policy around the adapter: retry
policy translation and overlapping retry loops have not been tested.

## Terminal results

A node error fails its child workflow and the graph workflow. With retries enabled,
exhausting the attempts raises DBOSMaxStepRetriesExceeded. A timed-out node raises
DBOSStepTimeoutError. Reading a completed or failed workflow handle again returns
the stored result or error; it does not run the node again.

This has been tested for ordinary errors, a transient failure that succeeds on the
third attempt, exhausted retries, and timeouts.

The retry budget is local to an execution of an unfinished step. It resets after
a process crash; it is not a lifetime limit on body executions. With max_attempts=3,
one attempt before a backoff crash followed by recovery can execute the body four
times. The backoff tests verify this for both success and exhausted retries. External
writes need idempotency even when the configured attempt count is small.

## Parallel node failure

When one parallel node fails, Pregel cancels the interrupted sibling coroutine.
The adapter then recursively cancels that sibling's DBOS child workflow. This
prevents the child from remaining PENDING and being revived at worker startup.
The root retains the original node error.

The test starts a blocked sibling before failing the other node. It checks that
the sibling exits without completing, its child status is CANCELLED, and restarting
the worker does not re-enter it. Already committed sibling work is not undone.
Failures while recording that cleanup, and nodes that suppress cancellation, still
need separate fault tests.

## Cancelling a graph

Cancel the root and its children:

```python
await DBOS.cancel_workflow_async(workflow_id, cancel_children=True)
```

The graph handle then reports cancellation. To stop a running async leaf body,
register it with `preemptible=True`. The test checks that the body exits through
its finally block after recursive cancellation. Cancelling only the root can leave
children running. Workflow-mode nodes must configure their own step preemption.

Cancellation does not roll back external writes. Blocking native calls or code
that suppresses asyncio cancellation may continue doing work. The tests exercise
an async body awaiting sleep; they do not establish cancellation behavior for
arbitrary tools or remote requests.

Changing these options for pending workflows changes their execution policy.
Treat that as a deployment compatibility decision; there is no automatic policy
migration or version check.
