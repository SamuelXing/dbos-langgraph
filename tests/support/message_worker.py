"""Recover a credential-free MessagesState model/tool loop."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage, messages_to_dict
from langgraph.graph import END, START, MessagesState, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def run(args):
    directory = Path(args.directory)

    def audit(name):
        with (directory / "calls").open("a") as stream:
            stream.write(name + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def message_id(value):
        return None if args.missing_ids else value

    async def model(state):
        if isinstance(state["messages"][-1], ToolMessage):
            audit("answer")
            return {
                "messages": [
                    AIMessage(
                        content=[{"type": "text", "text": "The answer is 42"}],
                        id=message_id("answer"),
                        response_metadata={"model": "fixture"},
                    )
                ]
            }
        audit("model")
        return {
            "messages": [
                AIMessage(
                    content="",
                    id=message_id("call"),
                    tool_calls=[
                        {
                            "name": "lookup",
                            "args": {"key": "answer"},
                            "id": "lookup-1",
                            "type": "tool_call",
                        }
                    ],
                )
            ]
        }

    async def tool(state):
        audit("tool")
        call = state["messages"][-1].tool_calls[0]
        assert call["id"] == "lookup-1"
        (directory / "entered").touch()
        while not (directory / "release").exists():
            await asyncio.sleep(0.01)
        return {
            "messages": [
                ToolMessage(
                    content="42",
                    tool_call_id=call["id"],
                    id=message_id("tool"),
                    artifact={"source": "fixture"},
                )
            ]
        }

    def route(state):
        return "tool" if state["messages"][-1].tool_calls else END

    builder = StateGraph(MessagesState)
    builder.add_node("model", model, metadata={"execute_in": "step"})
    builder.add_node("tool", tool, metadata={"execute_in": "step"})
    builder.add_edge(START, "model")
    builder.add_conditional_edges("model", route, ["tool", END])
    builder.add_edge("tool", "model")
    app = DBOSLangGraphPlugin("message-loop").register_graph(builder)
    DBOS(
        config={
            "name": "message-tests",
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
                handle = await app.start(
                    {"messages": [HumanMessage(content="Find the answer", id=message_id("user"))]}
                )
        result = await asyncio.wait_for(handle.get_result(), 30)
        print(json.dumps(messages_to_dict(result["messages"])), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--recover", action="store_true")
    parser.add_argument("--missing-ids", action="store_true")
    asyncio.run(run(parser.parse_args()))
