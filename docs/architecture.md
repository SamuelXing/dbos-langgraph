# Architecture

The package retains native LangGraph Pregel. It wraps registered graph execution in
a DBOS workflow and isolates each node invocation into its own child workflow.
Default leaf nodes use one durable step. Workflow-mode nodes explicitly orchestrate
DBOS steps within their own child history.

This avoids unconstrained multistep interleaving in one parent history. It does not
remove the determinism requirement for graph routing or parent durable-call ordering.
Simple Send and fixed BSP cases have test evidence; arbitrary graph scheduling does not.

The public facade provides ainvoke/start, not full CompiledStateGraph compatibility.
DBOS initialization, launch/destroy, executor IDs, application versions, recovery
services, and database ownership remain application responsibilities.

Names are stable ASCII identifiers under `dbos_langgraph.<graph>.<node>` and form
part of the persistence contract. Do not rename or change durable-call order for
pending histories without a compatible migration/version strategy.

The package does not checkpoint graph state independently. Replay reconstructs
Pregel state from durable node results. Side effects before step result commitment
can be repeated, requiring external idempotency.

Tests/support is intentionally excluded from the wheel. It contains audit and blocking
probes, a mock agent, and the original pure executor as an independent state reference.
