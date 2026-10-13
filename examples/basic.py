"""Run with: DBOS_SYSTEM_DATABASE_URL=sqlite:///example.sqlite python examples/basic.py."""

import asyncio
import os
import uuid

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def answer(state):
    return {"answer": f"Hello, {state['name']}"}


async def main():
    DBOS(
        config={
            "name": "dbos-langgraph-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "enable_otlp": False,
        }
    )
    plugin = DBOSLangGraphPlugin("example_v1")
    builder = StateGraph(dict)
    plugin.add_node(builder, "answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    app = plugin.compile(builder)
    DBOS.launch()
    try:
        with SetWorkflowID(str(uuid.uuid4())):
            handle = await app.start({"name": "World"})
        print(await handle.get_result())
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
