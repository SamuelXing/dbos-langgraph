# Recovery operations

## Determine whether a worker is dead

DBOS recovery re-enqueues histories assigned to executor IDs that the recovery
caller declares dead. It does not prove that their processes have stopped.

Stop or isolate an executor before recovering its histories. Do not use a database
connection failure, a missed heartbeat, or a timed-out request alone as proof that
its node bodies cannot continue. Give live workers distinct executor IDs and retain
compatible code for pending histories.

The live-worker probe holds the original invocation inside a node while a
replacement requests recovery for its executor ID. The original invocation then
returns a deliberately different update after ownership changes. The pinned SDK
rejects that stale execution's completion, and both callers receive the expected
graph state.

Recovered jobs can run on any eligible worker, including the original process.
The probe therefore tags only the first invocation as stale; tagging an entire
process would also change newly claimed work and invalidate the test.

This is one ownership-fencing case, not support for recovering live workers. It
does not prevent the old body from making external writes before its result is
rejected. It does not cover partitions, extended outages, or crashes during the
ownership handoff. Keep the dead-executor prerequisite in deployment procedures.

## Connection failures

The connection-fault test tags one worker's database connections with a unique
PostgreSQL application_name. It terminates only those sessions while a node is held,
then releases the node. The SDK reconnects and completes the graph. The test checks
that each node body executes once; it does not stop PostgreSQL or affect untagged
sessions.

This covers abrupt connection loss at one boundary, not a prolonged database outage
or a partition between workers. A successful reconnect test is not evidence that
live-worker takeover is safe. Do not trigger recovery merely because a worker has
lost its database connection.

## Losing connectivity while storing a node result

Two commit-outage tests put one worker behind a local connection relay. After a
sibling has committed, the relay drops existing sessions and rejects new ones.
The held node is released and returns its update while that connection path stays
unavailable for three seconds. A separate inspection connection checks that the
root remains PENDING and the returned node's step output has not been stored.
PostgreSQL remains running throughout.

When the relay is restored with the worker still alive, the SDK stores the result
and completes the graph without another body execution. When the worker is killed
before connectivity returns, recovery runs that unfinished body again and reuses
the committed sibling. A returned value alone is therefore not evidence of a
committed durable result. External effects made before result commitment need
idempotency.

These are bounded connection-path outages at one commit boundary. They do not
cover long-duration outages, PostgreSQL restarts, or asymmetric partitions between
multiple workers. Lost acknowledgments have separate tests below.

## Losing the commit acknowledgment

Two acknowledgment tests intercept the COMMIT for a known node-step transaction.
The relay sends that command to PostgreSQL but holds the response. A separate
connection must see the stored step output while the graph root is still PENDING.
Only then does the test discard the acknowledgment.

The live-worker case reconnects and finishes with one body execution per node.
The crash case kills the worker before it receives confirmation; recovery also
reuses the committed output without rerunning that body. This differs from losing
the worker before result commitment, where the unfinished body runs again.

The probe is tied to the pinned psycopg/SQLAlchemy transaction stream. It buffers
client packets to identify the known INSERT and COMMIT, and is restricted to a
local test database without TLS. It is not a general PostgreSQL protocol parser.
A dependency change that alters that stream must update the probe and rerun it.
These cases cover one stored node result, not arbitrary application transactions
or external tool writes.


## Workflow IDs and request identity

The adapter preserves DBOS workflow-ID reuse semantics. A root ID refers to one
stored invocation. Starting that ID again returns its original result, even if
the caller supplies different state. Node replay fingerprints check recovered
node inputs; they do not reject conflicting root requests.

Use a new ID for a new logical request. When retrying a request, retain both its
ID and its original input. Do not let an unscoped client-supplied ID identify
requests from different tenants.

If a service must reject changed input under the same request ID, bind the ID to
a canonical input fingerprint atomically in the service's request store. A unique
constraint or equivalent atomic operation must decide the first binding; later
requests compare against that stored value before starting the graph. A separate
read followed by a write leaves concurrent conflicting requests unchecked. Include
the graph and execution options in that identity when they affect the request.
The adapter currently supplies no request store or input-conflict rejection API.

Cross-process tests cover duplicates against a pending history and two workers
racing first creation on SQLite and PostgreSQL. With conflicting inputs, either
creator can win; every caller receives that winner's result. Two PostgreSQL cases
kill the original owner while duplicate callers wait, then recover its history.
The unfinished node executes again using the original input, even when waiting
callers supplied different inputs. Duplicate starts alone do not declare a dead
executor or trigger its recovery.

Initialize the SQLite schema before launching concurrent workers. Concurrent
cold launches against an empty database exposed a migration-table creation race
in DBOS 3.2.0. The workflow creation tests serialize SDK initialization and release
both initialized workers at the start barrier.


## Interrupted cancellation

Call DBOS.cancel_workflow_async(root_id, cancel_children=True) to cancel the graph
and its child histories. In DBOS 3.2.0, recursive cancellation commits each level
separately. A cancelled root does not prove that every descendant is cancelled.

A crash probe holds the SDK after the root transaction commits and before it
finds the children. After SIGKILL, a replacement observes a CANCELLED root and a
PENDING child. Repeating recursive cancellation before requesting dead-executor
recovery closes that gap. The root and child remain CANCELLED, repeated result
reads raise DBOSAwaitedWorkflowCancelledError, and the node body does not rerun.
If a cancellation request is interrupted, retain its root ID and retry the full
recursive request before recovering that executor's histories. A direct SDK cancellation call does not store a separate durable request.

A second probe waits for recursive cancellation to return and holds a preemptible
node inside its finally block. Killing the process during that cleanup leaves
the root and child CANCELLED on restart. The cleanup does not resume. Put required
cleanup effects in a separately tracked, retryable operation; a node's finally
block alone cannot guarantee their completion across process death.

These cases pass on SQLite and PostgreSQL. The gap probe intercepts a private SDK
storage helper only in the test process. They cover one child and explicit request
retry, not arbitrary hierarchy depth, automatic cancellation reconciliation, or
rollback of external effects.


## Durable cancellation requests

DurableGraph.request_cancel(target_id) starts a separate registered workflow named
`dbos_langgraph.<graph>.cancel`. It verifies that the target exists and names this
graph's root workflow, then invokes the SDK's recursive cancellation operation.
The SDK records that operation only after the full cascade returns. Recovery of
an interrupted request therefore repeats the unfinished operation.

Call request_cancel outside a DBOS workflow so the control request is independent
of the target's child hierarchy. Assign it a distinct stable ID with SetWorkflowID
and retain that ID alongside the target ID. Request IDs have the same first-input
reuse semantics as graph IDs: do not reuse one to cancel a different target.
Target validation is not tenant authorization; enforce access in the calling service.
Successful or failed target histories keep their terminal status under SDK rules.

After a worker crash, register the same graph before launching the replacement.
Use a distinct executor ID. Resume the pending control request with
DBOS.resume_workflow_async(request_id) and await its result before requesting
recovery of the dead executor's remaining histories. Do not resume the cancelled
graph itself. Recovering the control request and its target children together can
race cancellation against unfinished child execution. The adapter does not enforce
this recovery priority or discover pending cancellation requests for a service.

Crash tests resume the stored request through that public SDK API, without issuing
a new cancellation call from the replacement. They cover the root/child transaction
gap and a crash during cleanup, and reject a missing target. Interrupted cleanup
still needs a separate retryable operation.
