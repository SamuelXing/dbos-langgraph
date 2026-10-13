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
cover long-duration outages, PostgreSQL restarts, ambiguous commit acknowledgments,
or asymmetric partitions between multiple workers.
