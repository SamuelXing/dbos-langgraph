"""Reusable plugin acceptance sample, using the existing crash probes."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from dbos_langgraph import DBOSLangGraphPlugin

from .example import GRAPH, initial
from .pregel_dbos import MapState, State, audit, checked_phase


class ProbePlugin(DBOSLangGraphPlugin):
    def _node_completed(self, name, state, result):
        key = (
            name + "2"
            if name in ("b", "c")
            else "tail"
            if name == "tail"
            else f"map:{state['item']}"
            if name == "map"
            else f"{state['round'] * 3 if name == 'plan' else (state['round'] - 1) * 3 + (1 if name in ('fast', 'slow') else 2)}:{name}"
        )
        if os.environ.get("GRAPH_COMMITS"):
            with open(os.environ["GRAPH_COMMITS"], "a") as stream:
                stream.write(json.dumps({"key": key}) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        if key == os.environ.get("GRAPH_READY_KEY") or (
            name == "b" and os.environ.get("GRAPH_READY")
        ):
            Path(os.environ["GRAPH_READY"]).write_text("child workflow committed")


def build(mode):
    plugin = ProbePlugin("acceptance-" + mode)
    graph = StateGraph(State if mode == "cycle" else MapState)
    if mode == "cycle":
        for name in GRAPH.nodes:

            def factory(name):
                async def node(state):
                    index = (
                        state["round"] * 3
                        if name == "plan"
                        else (state["round"] - 1) * 3 + (1 if name in ("fast", "slow") else 2)
                    )
                    key = f"{index}:{name}"
                    audit(key)
                    if key == os.environ.get("GRAPH_BLOCK_KEY"):
                        await asyncio.sleep(3600)
                    return await GRAPH.nodes[name](state)

                return node

            plugin.add_node(graph, name, factory(name))
        graph.add_edge(START, "plan")
        graph.add_edge("plan", "fast")
        graph.add_edge("plan", "slow")
        graph.add_edge(["fast", "slow"], "decide")

        async def route(state):
            return "plan" if state["round"] < state["limit"] else END

        graph.add_conditional_edges("decide", route)
    elif mode == "dynamic":

        async def mapper(state):
            key = f"map:{state['item']}"
            audit(key)
            if key == os.environ.get("GRAPH_BLOCK_KEY"):
                await asyncio.sleep(3600)
            await asyncio.sleep(0.01 if state["item"] % 2 else 0.03)
            return {"score": state["item"], "events": [str(state["item"])]}

        plugin.add_node(graph, "map", mapper)

        async def route(state):
            return [Send("map", {"item": i}) for i in state["items"]]

        graph.add_conditional_edges(START, route)
        graph.add_edge("map", END)
    elif mode == "isolated":
        for name, delay in [("b", 0.05), ("c", 0.01)]:

            def factory(name, delay):
                async def node(state):
                    await checked_phase(name + "1", delay)
                    if name == "c" and os.environ.get("GRAPH_REPLAY_JITTER"):
                        await asyncio.sleep(0.15)
                    await checked_phase(name + "2", 0)
                    return {"events": [name]}

                return node

            plugin.add_node(graph, name, factory(name, delay), execute_in="workflow")

        async def end(state):
            audit("tail")
            if os.environ.get("GRAPH_BLOCK_KEY") == "tail":
                await asyncio.sleep(3600)
            return {}

        plugin.add_node(graph, "tail", end)
        graph.add_edge(START, "b")
        graph.add_edge(START, "c")
        graph.add_edge(["b", "c"], "tail")
        graph.add_edge("tail", END)
    else:
        raise ValueError("unsupported sample mode")
    return plugin.compile(graph)


async def run(args):
    apps = {mode: build(mode) for mode in ("cycle", "dynamic", "isolated")}
    app = apps[args.mode]
    DBOS(
        config={
            "name": "plugin-acceptance",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": "plugin-" + args.id,
            "application_version": "plugin-v0-" + args.id,
            "enable_otlp": False,
            "log_level": "WARNING",
        }
    )
    DBOS.launch()
    try:
        if args.command == "start":
            state = (
                initial(args.rounds)
                if args.mode == "cycle"
                else {"items": [1, 2, 3, 4], "score": 0, "events": []}
            )
            with SetWorkflowID(args.id):
                handle = await app.start(state)
        else:
            handle = await DBOS.retrieve_workflow_async(args.id)
        print(json.dumps({"state": await asyncio.wait_for(handle.get_result(), 60)}), flush=True)
    finally:
        DBOS.destroy()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start", "recover"])
    parser.add_argument("--mode", choices=["cycle", "dynamic", "isolated"], default="cycle")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--id", required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
