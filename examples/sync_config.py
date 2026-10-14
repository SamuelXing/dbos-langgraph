"""Run sync LangChain code with persisted config and runtime context."""

import asyncio
import os
import uuid
from typing import TypedDict

from dbos import DBOS, SetWorkflowID
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

from dbos_langgraph import DBOSLangGraphPlugin


class State(TypedDict, total=False):
    answer: str
    service: str


class Context(TypedDict):
    user: str


prompt = ChatPromptTemplate.from_template("{greeting} {user}")


def answer(state: State, config: RunnableConfig, runtime: Runtime[Context]):
    result = prompt.invoke(
        {"greeting": config["configurable"]["greeting"], "user": runtime.context["user"]},
        config=config,
    )
    return {"answer": result.messages[0].content}


async def finish(state: State, config: RunnableConfig):
    return {"service": config["metadata"]["service"]}


async def main():
    builder = StateGraph(State, context_schema=Context)
    builder.add_node("answer", answer, metadata={"execute_in": "step"})
    builder.add_node("finish", finish, metadata={"execute_in": "step"})
    builder.add_edge(START, "answer")
    builder.add_edge("answer", "finish")
    builder.add_edge("finish", END)
    app = DBOSLangGraphPlugin("sync_config_v1").register_graph(builder)
    DBOS(
        config={
            "name": "sync-config-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "enable_otlp": False,
            "log_level": "WARNING",
        }
    )
    DBOS.launch()
    try:
        config: RunnableConfig = {
            "configurable": {"greeting": "Hello"},
            "tags": ["example"],
            "metadata": {"service": "demo"},
            "recursion_limit": 8,
        }
        with SetWorkflowID(str(uuid.uuid4())):
            result = await app.ainvoke({}, config, context={"user": "Alice"})
        print(result)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
