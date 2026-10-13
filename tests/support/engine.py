"""Pure BSP scheduling; persistence belongs to the supplied node runner."""

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

State = dict[str, Any]


@dataclass(frozen=True)
class Graph:
    nodes: dict[str, Callable[[State], Awaitable[State]]]
    route: Callable[[str, State], list[str]]
    entry: tuple[str, ...]
    reducers: dict[str, Callable[[Any, Any], Any]] = field(default_factory=dict)

    def validate(self):
        if not self.entry or any(n not in self.nodes for n in self.entry):
            raise ValueError("entry must contain registered nodes")


def merge(state: State, updates: list[State], reducers: dict) -> State:
    result = deepcopy(state)
    writers = set()
    for update in updates:
        if not isinstance(update, dict):
            raise TypeError("nodes must return state update dictionaries")
        for key, value in update.items():
            if key in reducers:
                if key not in result:
                    raise ValueError(f"reducer key requires initial value: {key}")
                result[key] = reducers[key](result[key], deepcopy(value))
            else:
                if key in writers:
                    raise ValueError(f"parallel writes require reducer: {key}")
                result[key] = deepcopy(value)
            writers.add(key)
    return result


async def execute(graph: Graph, initial: State, runner=None, max_supersteps=1000):
    graph.validate()
    state = deepcopy(initial)
    active = sorted(set(graph.entry))
    trace = []
    for index in range(max_supersteps):
        if not active:
            return {"state": state, "trace": trace}
        snapshot = deepcopy(state)
        # Construct coroutines in canonical order, with independent snapshots.
        calls = [
            runner(name, deepcopy(snapshot), index)
            if runner
            else graph.nodes[name](deepcopy(snapshot))
            for name in active
        ]
        outcomes = await asyncio.gather(*calls, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        state = merge(state, outcomes, graph.reducers)
        next_nodes = sorted(
            {target for name in active for target in graph.route(name, deepcopy(state))}
        )
        if any(name not in graph.nodes for name in next_nodes):
            raise ValueError("route returned unregistered node")
        trace.append(
            {"index": index, "active": active, "next": next_nodes, "state": deepcopy(state)}
        )
        active = next_nodes
    if not active:
        return {"state": state, "trace": trace}
    raise RuntimeError("superstep limit exceeded")
