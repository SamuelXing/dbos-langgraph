"""A credential-free model/tool loop using native LangChain messages."""

import asyncio
import os
import uuid

from dbos import DBOS, SetWorkflowID
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph

from dbos_langgraph import DBOSLangGraphPlugin


async def model(state: MessagesState):
    # Replace this deterministic stand-in with your chat model's async invocation.
    if isinstance(state["messages"][-1], ToolMessage):
        return {"messages": [AIMessage(content="The answer is " + state["messages"][-1].content)]}
    return {
        "messages": [
            AIMessage(
                content="",
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


async def tools(state: MessagesState):
    call = state["messages"][-1].tool_calls[0]
    return {"messages": [ToolMessage(content="42", tool_call_id=call["id"])]}


def route(state: MessagesState):
    return "tools" if state["messages"][-1].tool_calls else END


async def main():
    builder = StateGraph(MessagesState)
    builder.add_node("model", model, metadata={"execute_in": "step"})
    builder.add_node("tools", tools, metadata={"execute_in": "step"})
    builder.add_edge(START, "model")
    builder.add_conditional_edges("model", route, ["tools", END])
    builder.add_edge("tools", "model")
    app = DBOSLangGraphPlugin("messages_agent_v1").register_graph(builder)
    DBOS(
        config={
            "name": "messages-agent-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "enable_otlp": False,
            "log_level": "WARNING",
        }
    )
    DBOS.launch()
    try:
        with SetWorkflowID(str(uuid.uuid4())):
            result = await app.ainvoke({"messages": [HumanMessage(content="Find the answer")]})
        for message in result["messages"]:
            print(message.type, message.id, message.content)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
