"""Use an existing builder with LangChain prompts and isolated runtime context."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import TypedDict

from dbos import DBOS, SetWorkflowID
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

from dbos_langgraph import DBOSLangGraphPlugin, ReplayMismatchError


class State(TypedDict, total=False):
    answer: str
    done: bool


class Context(TypedDict):
    tenant: str


async def run(args):
    directory = Path(args.directory)
    prompt = ChatPromptTemplate.from_template("Hello {tenant}")

    def audit(name):
        with (directory / "calls").open("a") as stream:
            stream.write(name + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def agent(state: State, runtime: Runtime[Context]):
        audit("agent")
        value = await prompt.ainvoke({"tenant": runtime.context["tenant"]})
        runtime.context["tenant"] = "local mutation"
        return {"answer": value.messages[0].content}

    async def finish(state: State, *, runtime: Runtime[Context]):
        audit("finish")
        assert runtime.context["tenant"] == "Alice"
        (directory / "entered").touch()
        while not (directory / "release").exists():
            await asyncio.sleep(0.01)
        return {"done": True}

    builder = StateGraph(State, context_schema=Context)
    builder.add_node("agent", agent, metadata={"execute_in": "step"})
    builder.add_node("finish", finish, metadata={"execute_in": "step"})

    def route(state: State, runtime: Runtime[Context]):
        if os.environ.get("CONTEXT_DRIFT"):
            runtime.context["tenant"] = "Bob"
        return "agent"

    builder.add_conditional_edges(START, route, ["agent"])
    builder.add_conditional_edges("agent", lambda state: "finish", ["finish"])
    builder.add_edge("finish", END)
    originals = {name: spec.runnable for name, spec in builder.nodes.items()}
    schemas = dict(builder.schemas)
    channels = dict(builder.channels)
    app = DBOSLangGraphPlugin("context-example").register_graph(builder)
    assert not builder.compiled
    assert builder.schemas == schemas
    assert builder.channels == channels
    assert all(builder.nodes[name].runnable is runnable for name, runnable in originals.items())
    DBOS(
        config={
            "name": "context-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id,
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        if args.recover:
            handle = await DBOS.retrieve_workflow_async(args.id)
        else:
            with SetWorkflowID(args.id):
                handle = await app.start({}, context={"tenant": "Alice"})
        try:
            result = await asyncio.wait_for(handle.get_result(), 30)
        except ReplayMismatchError:
            result = {"error": "ReplayMismatchError"}
        print(json.dumps(result), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--recover", action="store_true")
    asyncio.run(run(parser.parse_args()))
