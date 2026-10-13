"""Experimental native Pregel adapter. One durable step per leaf node."""

import asyncio
import json
import operator
import os
from copy import deepcopy
from pathlib import Path
from typing import Annotated, TypedDict

from dbos import DBOS
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from .example import GRAPH


def audit(key):
    if os.environ.get("GRAPH_AUDIT"):
        with open(os.environ["GRAPH_AUDIT"], "a") as stream:
            stream.write(json.dumps({"key": key}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


@DBOS.step()
async def leaf(key, name, state):
    audit(key)
    if key == os.environ.get("GRAPH_BLOCK_KEY"):
        await asyncio.sleep(3600)
    if name == "map":
        await asyncio.sleep(0.01 if state["item"] % 2 else 0.03)
        result = {"events": [str(state["item"])], "score": state["item"]}
    else:
        result = await GRAPH.nodes[name](deepcopy(state))
    return {"key": key, "result": result}


async def invoke_leaf(key, name, state):
    saved = await leaf(key, name, state)
    if saved["key"] != key:
        raise RuntimeError(f"history identity mismatch: requested {key}, replayed {saved['key']}")
    if os.environ.get("GRAPH_COMMITS"):
        with open(os.environ["GRAPH_COMMITS"], "a") as stream:
            stream.write(json.dumps({"key": key}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    if key == os.environ.get("GRAPH_READY_KEY"):
        Path(os.environ["GRAPH_READY"]).write_text("step output committed")
    return saved["result"]


class State(TypedDict):
    round: int
    limit: int
    score: Annotated[int, operator.add]
    events: Annotated[list[str], operator.add]


def cycle():
    graph = StateGraph(State)
    for name in GRAPH.nodes:

        async def node(state, name=name):
            index = (
                state["round"] * 3
                if name == "plan"
                else (state["round"] - 1) * 3 + (1 if name in ("fast", "slow") else 2)
            )
            return await invoke_leaf(f"{index}:{name}", name, state)

        graph.add_node(name, node)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "fast")
    graph.add_edge("plan", "slow")
    graph.add_edge(["fast", "slow"], "decide")

    async def route(state):
        return "plan" if state["round"] < state["limit"] else END

    graph.add_conditional_edges("decide", route)
    return graph.compile()  # No LangGraph checkpointer, including InMemorySaver.


class MapState(TypedDict):
    items: list[int]
    score: Annotated[int, operator.add]
    events: Annotated[list[str], operator.add]


def dynamic():
    graph = StateGraph(MapState)

    async def route(state):
        return [Send("map", {"item": item}) for item in state["items"]]

    async def mapper(state):
        return await invoke_leaf(f"map:{state['item']}", "map", state)

    graph.add_node("map", mapper)
    graph.add_conditional_edges(START, route)
    graph.add_edge("map", END)
    return graph.compile()


# Deliberately unsafe: multiple parallel sequences in one DBOS workflow.
@DBOS.step()
async def phase(key, delay):
    audit(key)
    await asyncio.sleep(delay)
    return key


async def checked_phase(key, delay):
    result = await phase(key, delay)
    if result != key:
        raise RuntimeError(f"history identity mismatch: requested {key}, replayed {result}")
    return result


@DBOS.step()
async def tail():
    audit("tail")
    if os.environ.get("GRAPH_BLOCK_KEY") == "tail":
        await asyncio.sleep(3600)
    return {}


@DBOS.workflow()
async def sequence_workflow(name, delay):
    await checked_phase(name + "1", delay)
    if name == "c" and os.environ.get("GRAPH_REPLAY_JITTER"):
        await asyncio.sleep(0.15)  # Test-only adversarial scheduling perturbation.
    await checked_phase(name + "2", 0)
    return {"events": [name]}


def unsafe(isolated=False):
    graph = StateGraph(MapState)
    for name, delay in [("b", 0.05), ("c", 0.01)]:

        async def node(state, name=name, delay=delay):
            if isolated:
                result = await sequence_workflow(name, delay)
            else:
                await checked_phase(name + "1", delay)
                if name == "c" and os.environ.get("GRAPH_REPLAY_JITTER"):
                    await asyncio.sleep(0.15)
                await checked_phase(name + "2", 0)
                result = {"events": [name]}
            if name == "b" and os.environ.get("GRAPH_READY"):
                Path(os.environ["GRAPH_READY"]).write_text("both sequences complete")
            return result

        graph.add_node(name, node)

    async def end(state):
        return await tail()

    graph.add_node("tail", end)
    graph.add_edge(START, "b")
    graph.add_edge(START, "c")
    graph.add_edge(["b", "c"], "tail")
    graph.add_edge("tail", END)
    return graph.compile()
