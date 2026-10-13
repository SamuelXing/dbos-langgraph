"""Run an existing StateGraph with a LangChain prompt and JSON runtime context."""

import asyncio
import os
import uuid
from typing import TypedDict

from dbos import DBOS, SetWorkflowID
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

from dbos_langgraph import DBOSLangGraphPlugin


class State(TypedDict):
    answer: str


class Context(TypedDict):
    user: str


prompt = ChatPromptTemplate.from_template("Hello {user}")


async def answer(state: State, runtime: Runtime[Context]):
    result = await prompt.ainvoke({"user": runtime.context["user"]})
    return {"answer": result.messages[0].content}


async def main():
    builder = StateGraph(State, context_schema=Context)
    builder.add_node("answer", answer, metadata={"execute_in": "step"})
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    app = DBOSLangGraphPlugin("runtime_context_v1").register_graph(builder)
    DBOS(
        config={
            "name": "runtime-context-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    try:
        with SetWorkflowID(str(uuid.uuid4())):
            handle = await app.start({}, context={"user": "Alice"})
        print(await handle.get_result())
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
