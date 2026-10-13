# Execution model

The adapter uses LangGraph's native Pregel runtime for scheduling, routing, reducers,
and joins. `compile()` wraps the graph in a DBOS workflow without a LangGraph
checkpointer.

A node invocation starts a child workflow. In step mode, the node body is one DBOS
step. In workflow mode, the node body can call several DBOS steps within that
child's history. This prevents parallel nodes from sharing a step counter.

During recovery, Pregel reconstructs state from saved node updates. The adapter
checks an input fingerprint before applying each recovered update. See
[replay and code changes](replay.md) for the check and its limits.

The application initializes DBOS, registers its graphs, launches workers, and
manages recovery ownership. The adapter exposes `ainvoke()` and `start()`; it does
not implement the full CompiledStateGraph interface.

Registered names use `dbos_langgraph.<graph>.<node>` and are part of stored history.
Keep names and call order stable while workflows are pending. An unfinished step
can run again, so external writes need idempotency or suitable transactions.

Test fixtures live under `tests/support` and are excluded from the wheel. They
include blocking probes, execution counters, and a pure executor used to check
reconstructed state independently of Pregel.


Existing builders are registered through register_graph, which copies the builder
and replaces each supported callable node's runnable while retaining its native
schema and metadata. This uses the pinned LangGraph internal RunnableCallable
wrapper; composed runnables are rejected rather than extracting an inner function.

JSON runtime context is part of the root input and the node replay fingerprint.
The leaf reconstructs Runtime.context from an isolated copy. Other Runtime fields
are not forwarded. Histories created without context retain their original root
argument shape, child argument shape, and input fingerprint format.
