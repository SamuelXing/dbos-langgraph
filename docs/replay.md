# Replay and code changes

LangGraph schedules nodes and merges their updates. DBOS stores the results of the
workflow and step calls made during that execution. On recovery, LangGraph runs
again and rebuilds state from those stored results.

Each node has a separate child workflow. This keeps parallel multistep nodes from
sharing a step counter. The parent still identifies child calls by their position
in its history. Routing and the order of child calls must therefore remain stable.

## Input checks

A completed child returns its state update and a SHA-256 fingerprint of the graph
name, node name, execution mode, and input state. Before applying the update, the
adapter compares that fingerprint with the current invocation. A mismatch raises
`ReplayMismatchError` instead of applying an update produced for a different input.
The error includes hashes, not the input values.

State inputs and updates must contain JSON values: objects with string keys,
lists, strings, booleans, finite numbers, or null. Object key order does not affect
the fingerprint; list order and value types do. Tuples, custom objects, and NaN
are rejected. This check runs before a node starts and before its update is stored.

This is an input guard, not a stable task-ID implementation. Two invocations with
identical inputs have the same fingerprint. Changes to node code or reducers can
also change behavior without changing node inputs. The adapter cannot certify an
upgrade as replay-compatible.

## Changing an application

Keep the code and dependency versions used by pending workflows available until
those workflows finish. Register the same graph catalog before launching a worker.
Use a new graph name for incompatible changes, and keep workers for the old version
available to recover old histories. Renaming a graph alone does not route workers;
configure DBOS application versions and recovery ownership separately.

If recovery reports a mismatch, stop retrying the changed deployment and restore
the compatible code. Do not delete history to make the error disappear. Investigate
routing, input serialization, graph registration, and dependency changes first.
There is no history-migration API in this release. Version 0.1.0a2 changes the node
result format; use 0.1.0a1 to recover histories created by 0.1.0a1.

The recovery tests exercise cycles, joins, Send fan-out, and isolated multistep
nodes. A separate fault case reverses Send order after a crash and checks that
recovery reports a mismatch.

A PostgreSQL test covers competing recovery requests for a dead worker's history.
It uses distinct executor IDs and the SDK's internal recovery-service hook in the
test harness; the installed adapter does not call that hook. Recovery while an old
worker is still running, rolling upgrades, and database outages remain untested.
