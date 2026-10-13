import pytest
from langgraph.graph import StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


def test_reject_unsupported_node_signatures():
    plugin = DBOSLangGraphPlugin("validation")

    async def node(state, config):
        return {}

    with pytest.raises(TypeError, match="only state"):
        plugin.add_node(StateGraph(dict), "node", node)
    with pytest.raises(ValueError, match="execute_in"):
        plugin.add_node(StateGraph(dict), "node", node, execute_in="invalid")


def test_reject_unwrapped_nodes():
    async def raw(state):
        return {}

    builder = StateGraph(dict)
    builder.add_node("raw", raw)
    with pytest.raises(ValueError, match="all nodes"):
        DBOSLangGraphPlugin("unwrapped").compile(builder)


@pytest.mark.parametrize("name", ["", "has.dot", "has space", "\u4e2d\u6587"])
def test_reject_unstable_registration_names(name):
    with pytest.raises(ValueError, match="names"):
        DBOSLangGraphPlugin(name)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "10"])
@pytest.mark.asyncio
async def test_invalid_recursion_limit_never_starts_workflow(limit):
    from dbos_langgraph import DurableGraph

    async def workflow(*args):
        pytest.fail("invalid input must be rejected before executing")

    graph = DurableGraph(workflow)
    with pytest.raises(ValueError, match="positive integer"):
        await graph.ainvoke({}, recursion_limit=limit)
    with pytest.raises(ValueError, match="positive integer"):
        await graph.start({}, recursion_limit=limit)


def test_fingerprint_preserves_types_and_ignores_object_key_order():
    from dbos_langgraph._state import fingerprint

    assert fingerprint({"a": 1, "b": [True, None]}) == fingerprint({"b": [True, None], "a": 1})
    assert fingerprint([1]) != fingerprint([True])
    assert fingerprint([1, 2]) != fingerprint([2, 1])


@pytest.mark.parametrize(
    "state", [{1: "value"}, (1, 2), {"value": float("nan")}, {"value": object()}]
)
def test_fingerprint_rejects_ambiguous_state(state):
    from dbos_langgraph._state import fingerprint

    with pytest.raises(TypeError, match="JSON"):
        fingerprint(state)


@pytest.mark.parametrize(
    "options",
    [
        {"max_attempts": True},
        {"max_attempts": 0},
        {"interval_seconds": 0},
        {"backoff_rate": float("inf")},
        {"timeout_seconds": -1},
        {"preemptible": 1},
    ],
)
def test_step_options_reject_invalid_values(options):
    from dbos_langgraph import StepOptions

    with pytest.raises(ValueError):
        StepOptions(**options)


def test_workflow_nodes_cannot_take_leaf_step_options():
    from dbos_langgraph import StepOptions

    async def node(state):
        return {}

    with pytest.raises(ValueError, match="configure their own"):
        DBOSLangGraphPlugin("policy-validation").add_node(
            StateGraph(dict), "node", node, execute_in="workflow", step_options=StepOptions()
        )


@pytest.mark.parametrize("workflow_id", ["", "   ", None, 42])
@pytest.mark.asyncio
async def test_invalid_cancellation_target_never_starts_request(workflow_id):
    from dbos_langgraph import DurableGraph

    async def workflow(*args):
        pytest.fail("invalid cancellation must not start a workflow")

    graph = DurableGraph(workflow)
    with pytest.raises(ValueError, match="nonempty string"):
        await graph.request_cancel(workflow_id)


@pytest.mark.parametrize("owners", [[], [""], [None], "executor"])
@pytest.mark.asyncio
async def test_reconciliation_requires_explicit_dead_executor_ids(owners):
    from dbos_langgraph import DurableGraph

    async def workflow(*args):
        pytest.fail("invalid reconciliation must not start work")

    with pytest.raises(ValueError, match="confirmed-dead"):
        await DurableGraph(workflow).reconcile_cancellations(owners)


@pytest.mark.parametrize(
    "metadata", [{}, {"execute_in": "activity"}, {"execute_in": "step", "step_options": {}}]
)
def test_existing_graph_rejects_invalid_execution_metadata_without_mutation(metadata):
    async def node(state):
        return {}

    builder = StateGraph(dict)
    builder.add_node("node", node, metadata=metadata)
    original = builder.nodes["node"]
    with pytest.raises((ValueError, TypeError)):
        DBOSLangGraphPlugin("metadata-validation").register_graph(builder)
    assert builder.nodes["node"] is original
    assert not builder.compiled


@pytest.mark.asyncio
async def test_non_json_context_is_rejected_before_workflow_execution():
    from dbos_langgraph import DurableGraph

    async def workflow(*args, **kwargs):
        pytest.fail("invalid context must not start a workflow")

    app = DurableGraph(workflow)
    with pytest.raises(TypeError, match="JSON"):
        await app.start({}, context={"object": object()})
    with pytest.raises(TypeError, match="JSON"):
        await app.ainvoke({}, context={"object": object()})
