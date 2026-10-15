"""Start the same graph ID from independently launched DBOS executors."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def wait_for(path):
    async with asyncio.timeout(30):
        while not path.exists():
            await asyncio.sleep(0.01)


async def run(args):
    directory = Path(args.directory)

    async def node(state):
        # Separate files retain evidence even if an unexpected second executor runs.
        with (directory / f"calls-{args.worker}.jsonl").open("a") as audit:
            audit.write(json.dumps(state) + "\n")
            audit.flush()
            os.fsync(audit.fileno())
        (directory / "entered").touch()
        await wait_for(directory / "release")
        return {"value": state["value"] + 1}

    builder = StateGraph(dict)
    plugin = DBOSLangGraphPlugin("duplicate-process")
    plugin.add_node(builder, "node", node)
    builder.add_edge(START, "node")
    builder.add_edge("node", END)
    app = plugin.compile(builder)
    DBOS(
        config={
            "name": "duplicate-process-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": f"{args.id}-{args.worker}",
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        if args.race:
            (directory / f"ready-{args.worker}").touch()
            await wait_for(directory / "start")
        with SetWorkflowID(args.id):
            handle = await app.start({"value": args.value})
        (directory / f"started-{args.worker}").touch()
        if args.recover:
            await wait_for(directory / "recover")
            # The executor is known before launch. The gate only confirms death;
            # never read an ID from a file that may be visible before its write.
            owner = f"{args.id}-original"
            print(json.dumps({"recover_owner": owner}), flush=True)
            handles = await asyncio.to_thread(DBOS._recover_pending_workflows, [owner])
            print(json.dumps({"recovered_histories": len(handles)}), flush=True)
            if not handles:
                raise RuntimeError(f"no pending histories found for dead executor {owner}")
        result = await asyncio.wait_for(handle.get_result(), 30)
        print(json.dumps(result), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--value", type=int, required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--recover", action="store_true")
    asyncio.run(run(parser.parse_args()))
