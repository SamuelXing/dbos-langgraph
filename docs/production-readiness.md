# Before production use

The adapter is alpha. These are the remaining release requirements.

## Recovery

The current tests cover process crashes during cycles, parallel joins, Send fan-out,
and multistep nodes. They check final state and count actual node-body executions.
Changed Send inputs must produce a replay error.

Repeated-recovery tests kill a worker three times at the same unfinished node.
Two seed families vary completion delays on each restart for cycles and Send
fan-out. These are bounded scheduling samples, not a broad fault campaign.

A second test crashes the graph in three successive loop rounds. A PostgreSQL
case synchronizes recovery requests from two distinct workers for a dead worker's
history and checks results and node-body counts.

A scoped connection-termination test covers reconnecting after lost PostgreSQL
sessions. A live-worker probe verifies that an original invocation's altered update
does not replace the recovered result. Recovery still requires verified-dead
executors; fencing stored results does not fence external tool effects.

Proxy tests cover a three-second connection-path outage during result commitment,
with and without a worker crash. Lost-acknowledgment tests verify reuse after a
step commit is visible to a separate session but its response is withheld.
Duplicate root requests are covered within one process and across separate
executors: starts against an existing pending history execute one body, and
changed inputs reuse the original result. Two workers also race first creation
after SDK initialization; either input may win, but both receive one result.
PostgreSQL tests kill the original owner with duplicate callers waiting, recover
its history, and verify reuse of the original input. Larger contention samples
and crashes at other boundaries remain untested. Services requiring input-conflict
rejection need an atomic request-identity binding at their boundary.
Cancellation reconciliation covers two requests, a nested child workflow, and
a second coordinator crash. Services must still enforce exclusive coordination
and run reconciliation before general recovery. Long-duration outages remain untested.
Identical-input task identity remains an open question.

Ordinary errors, transient/exhausted retries, backoff crashes, per-attempt timeouts,
and recursive cancellation of a preemptible async leaf have focused tests. Parallel
failure tests cover one interrupted sibling and its status after restart. Arbitrary
tool cancellation remains untested. Crash probes cover the gap between root and
child cancellation and interruption inside leaf cleanup. A separate durable cancellation request can close the tested gap when recovered
before target histories; interrupted cleanup is not resumed. Deeper hierarchies
and durable cleanup operations still need coverage.

## LangGraph features

The supported API accepts plain sync/async nodes with state and optional config/runtime
arguments and dict updates. State accepts JSON and standard LangChain messages;
custom state objects and message shorthand need additional support. Runtime.context accepts JSON values. Config supports a persisted subset; callbacks
and other Runtime fields are unsupported. Sync execution requires threads and
cannot support preemption or per-attempt timeouts. Interrupt/resume, subgraphs, streaming, and Functional
API need separate implementations and recovery tests. Add them one at a time;
passing an ordinary invocation test is not sufficient.

## Deployments

Pending histories must survive a worker restart with compatible code. Rolling
upgrades and incompatible graph changes need executable tests and an operational
procedure. There is no history migration support yet.

CI is configured for Python 3.12/3.13 with SQLite and PostgreSQL. Hosted results
must be reviewed before declaring that matrix supported. Add trace correlation,
recovery timing, history-size measurements, and retention guidance before a
production release.

## Releases

Build and install the wheel in a clean environment. Record the tested dependency
versions, API changes, and history-format changes. Establish a security contact
and review the release before publishing.
