"""Reusable plugin acceptance sample, using the existing crash probes."""

import argparse
import asyncio
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from dbos_langgraph import DBOSLangGraphPlugin

from .example import GRAPH, initial
from .pregel_dbos import MapState, State, audit, checked_phase


async def scheduling_delay(key):
    """Vary body completion order without changing graph inputs or call order."""
    seed = os.environ.get("GRAPH_SCHEDULE_SEED")
    if seed is None:
        return
    digest = hashlib.sha256(f"{seed}:{key}".encode()).digest()
    await asyncio.sleep(int.from_bytes(digest[:2], "big") / 65535 * 0.05)


async def hold_recovery_node(key):
    if key != os.environ.get("GRAPH_GATE_KEY"):
        return
    release = Path(os.environ["GRAPH_GATE_RELEASE"])
    while not release.exists():
        await asyncio.sleep(0.01)


async def request_recovery():
    owner = os.environ.get("GRAPH_RECOVER_OWNER")
    if owner is None:
        return
    Path(os.environ["GRAPH_RECOVER_READY"]).write_text("ready")
    release = Path(os.environ["GRAPH_RECOVER_RELEASE"])
    while not release.exists():
        await asyncio.sleep(0.01)
    # Exercise the SDK recovery service path; this internal hook is test-only.
    await asyncio.to_thread(DBOS._recover_pending_workflows, [owner])
    Path(os.environ["GRAPH_RECOVER_DONE"]).write_text("requested")


def invocation_key(name, state):
    if name in ("b", "c"):
        return name + "2"
    if name == "tail":
        return "tail"
    if name == "map":
        return f"map:{state['item']}"
    return f"{cycle_index(name, state)}:{name}"


def cycle_index(name, state):
    if name == "plan":
        return state["round"] * 3
    offset = 1 if name in ("fast", "slow") else 2
    return (state["round"] - 1) * 3 + offset


class ProbePlugin(DBOSLangGraphPlugin):
    def _node_completed(self, name, state, result):
        key = invocation_key(name, state)
        if os.environ.get("GRAPH_COMMITS"):
            with open(os.environ["GRAPH_COMMITS"], "a") as stream:
                stream.write(json.dumps({"key": key}) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        ready_key_reached = key == os.environ.get("GRAPH_READY_KEY")
        sequence_finished = name == "b" and os.environ.get("GRAPH_READY")
        if ready_key_reached or sequence_finished:
            Path(os.environ["GRAPH_READY"]).write_text("child workflow committed")


def build(mode):
    plugin = ProbePlugin("acceptance-" + mode)
    graph = StateGraph(State if mode == "cycle" else MapState)
    if mode == "cycle":
        invocations = Counter()
        for name in GRAPH.nodes:

            def factory(name):
                async def node(state):
                    key = invocation_key(name, state)
                    invocations[key] += 1
                    invocation_number = invocations[key]
                    audit(key)
                    await scheduling_delay(key)
                    await hold_recovery_node(key)
                    if key == os.environ.get("GRAPH_BLOCK_KEY"):
                        await asyncio.sleep(3600)
                    result = await GRAPH.nodes[name](state)
                    if key == os.environ.get("GRAPH_GATE_KEY") and invocation_number == 1:
                        result["score"] = result.get("score", 0) + int(
                            os.environ.get("GRAPH_STALE_SCORE_OFFSET", "0")
                        )
                        finished = os.environ.get("GRAPH_BODY_FINISHED")
                        if finished:
                            Path(finished).write_text("body returned")
                    return result

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
            await scheduling_delay(key)
            if key == os.environ.get("GRAPH_BLOCK_KEY"):
                await asyncio.sleep(3600)
            await asyncio.sleep(0.01 if state["item"] % 2 else 0.03)
            return {"score": state["item"], "events": [str(state["item"])]}

        plugin.add_node(graph, "map", mapper)

        async def route(state):
            items = (
                list(reversed(state["items"]))
                if os.environ.get("GRAPH_INPUT_DRIFT")
                else state["items"]
            )
            return [Send("map", {"item": i}) for i in items]

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
    config = {
        "name": "plugin-acceptance",
        "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
        "executor_id": os.environ.get("GRAPH_EXECUTOR_ID", "plugin-" + args.id),
        "application_version": "plugin-v0-" + args.id,
        "enable_otlp": False,
        "log_level": "WARNING",
    }
    connection_name = os.environ.get("GRAPH_DB_APPLICATION_NAME")
    if connection_name:
        config["db_engine_kwargs"] = {"connect_args": {"application_name": connection_name}}
    DBOS(config=config)
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
            await request_recovery()
            handle = await DBOS.retrieve_workflow_async(args.id)
        result = await asyncio.wait_for(handle.get_result(), 60)
        exit_gate = os.environ.get("GRAPH_WORKER_EXIT_GATE")
        if exit_gate:
            while not Path(exit_gate).exists():
                await asyncio.sleep(0.01)
        print(json.dumps({"state": result}), flush=True)
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
