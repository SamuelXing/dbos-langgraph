"""Run two config-aware graphs with blocking sync nodes and isolated invocation data."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import TypedDict

from dbos import DBOS, SetWorkflowID
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

from dbos_langgraph import DBOSLangGraphPlugin


class State(TypedDict, total=False):
    value: int
    user: str
    done: bool


async def run(args):
    directory = Path(args.directory)

    def audit(name, user):
        with (directory / "calls").open("a") as stream:
            stream.write(f"{name}:{user}\n")
            stream.flush()
            os.fsync(stream.fileno())

    def plan(state, config: RunnableConfig, *, runtime: Runtime[dict]):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise AssertionError("sync node must run outside the event loop")
        assert DBOS.workflow_id is not None
        user = runtime.context["user"]
        factor = config["configurable"]["factor"]
        audit("plan", user)
        assert config["tags"] == [user]
        assert config["metadata"] == {"user": user}
        assert config["recursion_limit"] == 8
        time.sleep(0.02)
        config["configurable"]["factor"] = 999
        runtime.context["user"] = "local mutation"
        return {"value": factor, "user": user}

    async def finish(state, config: RunnableConfig, runtime: Runtime[dict]):
        user = runtime.context["user"]
        assert config["configurable"]["factor"] == state["value"]
        assert user == state["user"]
        audit("finish", user)
        (directory / f"ready-{user}").touch()
        while not (directory / "release").exists():
            await asyncio.sleep(0.01)
        return {"done": True}

    builder = StateGraph(State, context_schema=dict)
    builder.add_node("plan", plan, metadata={"execute_in": "step"})
    builder.add_node("finish", finish, metadata={"execute_in": "step"})
    builder.add_edge(START, "plan")
    builder.add_edge("plan", "finish")
    builder.add_edge("finish", END)
    app = DBOSLangGraphPlugin("config-example").register_graph(builder)
    DBOS(
        config={
            "name": "config-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id,
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        handles = []
        for user, factor in (("Alice", 2), ("Bob", 3)):
            identity = args.id + "-" + user
            if args.recover:
                handle = await DBOS.retrieve_workflow_async(identity)
            else:
                config = {
                    "configurable": {"factor": factor},
                    "tags": [user],
                    "metadata": {"user": user},
                    "recursion_limit": 8,
                }
                with SetWorkflowID(identity):
                    handle = await app.start({}, config, context={"user": user})
            handles.append(handle)
        results = await asyncio.wait_for(asyncio.gather(*(h.get_result() for h in handles)), 30)
        print(json.dumps(results), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--recover", action="store_true")
    asyncio.run(run(parser.parse_args()))
