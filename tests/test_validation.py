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
