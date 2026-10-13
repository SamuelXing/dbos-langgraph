"""Exercise SDK failures and cancellation in an isolated process."""

import argparse
import asyncio
import json
import os

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin, StepOptions


async def run(args):
    attempts = 0
    stopped = asyncio.Event()
    entered = asyncio.Event()
    options = StepOptions(max_attempts=3, interval_seconds=0.01)
    if args.mode == "cancel":
        options = StepOptions(preemptible=True)
    elif args.mode == "timeout":
        options = StepOptions(timeout_seconds=0.05)
    elif args.mode == "error":
        options = StepOptions()

    async def node(state):
        nonlocal attempts
        attempts += 1
        entered.set()
        if args.mode in ("cancel", "timeout"):
            try:
                await asyncio.sleep(3600)
            finally:
                stopped.set()
        if args.mode in ("error", "exhaust") or (args.mode == "retry" and attempts < 3):
            raise ValueError("transient node failure")
        return {"answer": "done"}

    graph = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("failure-" + args.mode)
    plugin.add_node(graph, "node", node, step_options=options)
    graph.add_edge(START, "node")
    graph.add_edge("node", END)
    app = plugin.compile(graph)
    DBOS(
        config={
            "name": "failure-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id,
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        with SetWorkflowID(args.id):
            handle = await app.start({})
        if args.mode == "cancel":
            await asyncio.wait_for(entered.wait(), 10)
            await DBOS.cancel_workflow_async(args.id, cancel_children=True)
        error, result = None, None
        try:
            result = await asyncio.wait_for(handle.get_result(), 15)
        except Exception as exc:
            error = type(exc).__name__
        if args.mode in ("cancel", "timeout"):
            await asyncio.wait_for(stopped.wait(), 10)
        # Reading a terminal result again must not rerun failed or completed bodies.
        second_error = None
        try:
            await asyncio.wait_for(handle.get_result(), 10)
        except Exception as exc:
            second_error = type(exc).__name__
        status = await handle.get_status()
        print(
            json.dumps(
                {
                    "attempts": attempts,
                    "result": result,
                    "error": error,
                    "second_error": second_error,
                    "status": status.status,
                    "stopped": stopped.is_set(),
                }
            ),
            flush=True,
        )
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["retry", "exhaust", "error", "cancel", "timeout"])
    parser.add_argument("--id", required=True)
    asyncio.run(run(parser.parse_args()))
