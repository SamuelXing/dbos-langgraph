"""A retry budget observed across process restarts."""

import argparse
import asyncio
import json
import os

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin, StepOptions

from .pregel_dbos import audit


async def run(args):
    attempts = 0

    async def node(state):
        nonlocal attempts
        attempts += 1
        audit("attempt")
        if args.outcome == "exhaust" or attempts < 3:
            raise ValueError("retry probe")
        return {"answer": "done"}

    builder = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("retry-crash-" + args.outcome)
    plugin.add_node(
        builder,
        "node",
        node,
        step_options=StepOptions(
            max_attempts=3,
            interval_seconds=2.0,
            backoff_rate=1.0,
        ),
    )
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    app = plugin.compile(builder)
    DBOS(
        config={
            "name": "retry-crash",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id,
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "WARNING",
        }
    )
    DBOS.launch()
    try:
        if args.command == "start":
            with SetWorkflowID(args.id):
                handle = await app.start({})
        else:
            handle = await DBOS.retrieve_workflow_async(args.id)
        error, result = None, None
        try:
            result = await asyncio.wait_for(handle.get_result(), 30)
        except Exception as exc:
            error = type(exc).__name__
        print(json.dumps({"error": error, "result": result, "attempts": attempts}), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start", "recover"])
    parser.add_argument("outcome", choices=["success", "exhaust"])
    parser.add_argument("--id", required=True)
    asyncio.run(run(parser.parse_args()))
