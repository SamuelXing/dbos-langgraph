"""Start, cancel, or reconcile the cancellable graph in a dedicated database."""

import argparse
import asyncio
import json
import os

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin, StepOptions


async def wait_for_work(state):
    await asyncio.sleep(3600)
    return {"finished": True}


async def main(args):
    plugin = DBOSLangGraphPlugin("cancellation_example_v1")
    builder = StateGraph(dict)
    plugin.add_node(builder, "wait", wait_for_work, step_options=StepOptions(preemptible=True))
    builder.add_edge(START, "wait")
    builder.add_edge("wait", END)
    app = plugin.compile(builder)
    DBOS(
        config={
            "name": "cancellation-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.executor,
            "application_version": "cancellation-example-v1",
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    try:
        if args.command == "start":
            with SetWorkflowID(args.id):
                handle = await app.start({})
            print(json.dumps({"graph_id": args.id, "executor": args.executor}), flush=True)
            try:
                print(await handle.get_result())
            except Exception:
                status = await handle.get_status()
                if status.status != "CANCELLED":
                    raise
                print("Graph cancelled", flush=True)
        elif args.command == "cancel":
            with SetWorkflowID(args.request_id):
                request = await app.request_cancel(args.target_id)
            await request.get_result()
            print(json.dumps({"request_id": args.request_id, "status": "SUCCESS"}))
        else:
            # Keep general executor recovery disabled until this call returns.
            completed = await app.reconcile_cancellations(args.dead_executor, batch_size=100)
            print(json.dumps({"completed_requests": completed}), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executor", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("id")
    cancel = commands.add_parser("cancel")
    cancel.add_argument("target_id")
    cancel.add_argument("request_id")
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--dead-executor", action="append", required=True)
    asyncio.run(main(parser.parse_args()))
