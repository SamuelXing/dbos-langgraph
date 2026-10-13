"""Hold recursive cancellation between transactions or inside leaf cleanup."""

import argparse
import asyncio
import json
import os
import threading
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin, StepOptions


async def run(args):
    directory = Path(args.directory)
    entered = asyncio.Event()

    async def node(state):
        with (directory / "calls").open("a") as audit:
            audit.write("entered\n")
            audit.flush()
            os.fsync(audit.fileno())
        entered.set()
        try:
            await asyncio.sleep(3600)
        finally:
            (directory / "cleanup").touch()
            await asyncio.Event().wait()
        return {}

    builder = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("cancellation-crash")
    plugin.add_node(builder, "node", node, step_options=StepOptions(preemptible=True))
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    app = plugin.compile(builder)
    instance = DBOS(
        config={
            "name": "cancellation-crash-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id + ("-replacement" if args.inspect else "-original"),
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        if args.inspect:
            before_root = await DBOS.get_workflow_status_async(args.id)
            before = await DBOS.list_workflows_async(parent_workflow_id=args.id)
            request_status = None
            if args.durable:
                request = await DBOS.resume_workflow_async(args.id + "-cancel")
                await asyncio.wait_for(request.get_result(), 15)
                request_status = (await request.get_status()).status
            else:
                # Retry the interrupted control request before declaring the old owner dead.
                await DBOS.cancel_workflow_async(args.id, cancel_children=True)
            await asyncio.to_thread(DBOS._recover_pending_workflows, [args.id + "-original"])
            handle = await DBOS.retrieve_workflow_async(args.id)
            errors = []
            for _ in range(2):
                try:
                    await asyncio.wait_for(handle.get_result(), 10)
                except Exception as exc:
                    errors.append(type(exc).__name__)
            await asyncio.sleep(0.2)
            root = await handle.get_status()
            children = await DBOS.list_workflows_async(parent_workflow_id=args.id)
            observation = {"request": request_status} if args.durable else {}
            if args.durable:
                invalid = await app.request_cancel(args.id + "-missing")
                try:
                    await invalid.get_result()
                except ValueError:
                    observation["missing_target"] = "rejected"
                else:
                    raise AssertionError("a missing target must fail the request")
            print(
                json.dumps(
                    {
                        **observation,
                        "before_root": before_root.status,
                        "before": [child.status for child in before],
                        "root": root.status,
                        "children": [child.status for child in children],
                        "errors": errors,
                        "entered": entered.is_set(),
                    }
                ),
                flush=True,
            )
            return
        with SetWorkflowID(args.id):
            await app.start({})
        await asyncio.wait_for(entered.wait(), 10)
        if args.boundary == "cascade":
            original = instance._sys_db._get_direct_children

            def held_children(ids):
                if args.id in ids:
                    # The root transaction has committed; descendants are still pending.
                    (directory / "gap").touch()
                    threading.Event().wait()
                return original(ids)

            instance._sys_db._get_direct_children = held_children
        if args.durable:
            with SetWorkflowID(args.id + "-cancel"):
                request = await app.request_cancel(args.id)
            await request.get_result()
        else:
            await DBOS.cancel_workflow_async(args.id, cancel_children=True)
        (directory / "cancel-returned").touch()
        await asyncio.Event().wait()
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--boundary", choices=["cascade", "cleanup"], required=True)
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--durable", action="store_true")
    asyncio.run(run(parser.parse_args()))
