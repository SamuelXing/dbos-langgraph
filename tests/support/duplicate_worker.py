"""Probe workflow-ID reuse while the original invocation is pending and complete."""

import argparse
import asyncio
import json
import os

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def run(args):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def node(state):
        calls.append(state["value"])
        entered.set()
        await release.wait()
        return {"value": state["value"] + 1}

    builder = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("duplicate")
    plugin.add_node(builder, "node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    app = plugin.compile(builder)
    DBOS(
        config={
            "name": "duplicate-tests",
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
            original = await app.start({"value": 1})
        await asyncio.wait_for(entered.wait(), 10)

        async def duplicate(value):
            with SetWorkflowID(args.id):
                return await app.start({"value": value})

        pending = await asyncio.gather(*(duplicate(args.value) for _ in range(8)))
        release.set()
        results = await asyncio.wait_for(
            asyncio.gather(original.get_result(), *(h.get_result() for h in pending)), 20
        )
        completed = await duplicate(args.value)
        results.append(await completed.get_result())
        with SetWorkflowID(args.id):
            results.append(await app.ainvoke({"value": args.value}))
        print(json.dumps({"calls": calls, "results": results}), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--value", required=True, type=int)
    asyncio.run(run(parser.parse_args()))
