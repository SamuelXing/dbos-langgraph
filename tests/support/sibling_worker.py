"""Observe a sibling child when a parallel node fails."""

import argparse
import asyncio
import json
import os

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def run(identity, inspect_only=False):
    entered = asyncio.Event()
    release = asyncio.Event()
    stopped = asyncio.Event()
    child_id = None
    finished = False

    async def slow(state):
        nonlocal child_id, finished
        child_id = DBOS.workflow_id
        entered.set()
        try:
            await release.wait()
            finished = True
            return {"slow": True}
        finally:
            stopped.set()

    async def fail(state):
        await entered.wait()
        raise ValueError("parallel node failed")

    builder = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("sibling-failure")
    plugin.add_node(builder, "slow", slow)
    plugin.add_node(builder, "fail", fail)
    builder.add_edge(START, "slow")
    builder.add_edge(START, "fail")
    builder.add_edge(["slow", "fail"], END)
    app = plugin.compile(builder)
    DBOS(
        config={
            "name": "sibling-failure",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": identity,
            "application_version": identity,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        if inspect_only:
            handle = await DBOS.retrieve_workflow_async(identity)
            error = None
            try:
                await asyncio.wait_for(handle.get_result(), 15)
            except Exception as exc:
                error = type(exc).__name__
            children = await DBOS.list_workflows_async(parent_workflow_id=identity)
            print(
                json.dumps(
                    {
                        "error": error,
                        "entered": entered.is_set(),
                        "children": sorted(child.status for child in children),
                    }
                ),
                flush=True,
            )
            return
        with SetWorkflowID(identity):
            handle = await app.start({})
        error = None
        try:
            await asyncio.wait_for(handle.get_result(), 15)
        except Exception as exc:
            error = type(exc).__name__
        stopped_at_failure = stopped.is_set()
        release.set()
        await asyncio.wait_for(stopped.wait(), 10)
        await asyncio.sleep(0.1)
        status = await DBOS.get_workflow_status_async(child_id)
        print(
            json.dumps(
                {
                    "error": error,
                    "stopped_at_failure": stopped_at_failure,
                    "finished": finished,
                    "child_status": status.status,
                }
            ),
            flush=True,
        )
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--inspect", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.id, args.inspect))
