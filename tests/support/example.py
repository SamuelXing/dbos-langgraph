"""A deterministic mock agent: plan -> parallel tools -> decide -> plan/end."""

import asyncio
import operator

from .engine import Graph


async def plan(state):
    return {"round": state["round"] + 1, "events": ["plan"]}


async def fast(state):
    await asyncio.sleep(0.01)
    return {"score": 1, "events": ["fast"]}


async def slow(state):
    await asyncio.sleep(0.03)
    return {"score": 2, "events": ["slow"]}


async def decide(state):
    return {"events": ["decide"]}


def route(name, state):
    if name == "plan":
        return ["slow", "fast"]
    if name in ("fast", "slow"):
        return ["decide"]
    return ["plan"] if state["round"] < state["limit"] else []


GRAPH = Graph(
    {"plan": plan, "fast": fast, "slow": slow, "decide": decide},
    route,
    ("plan",),
    {"score": operator.add, "events": operator.add},
)


def initial(limit=3):
    if limit < 1:
        raise ValueError("limit must be positive")
    return {"round": 0, "limit": limit, "score": 0, "events": []}
