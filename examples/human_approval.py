"""Run with DBOS_SYSTEM_DATABASE_URL=sqlite:///approval.sqlite python examples/human_approval.py."""

import asyncio
import os
import uuid
from typing import TypedDict

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from dbos_langgraph import DBOSLangGraphPlugin


class State(TypedDict, total=False):
    action: str
    approved: bool
    result: str


def approval(state):
    return {"approved": interrupt({"action": state["action"], "question": "Proceed?"})}


async def execute(state):
    return {"result": f"Completed: {state['action']}"}


async def main():
    DBOS(
        config={
            "name": "approval-example",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "enable_otlp": False,
        }
    )
    builder = StateGraph(State)
    builder.add_node("approval", approval, metadata={"execute_in": "step"})
    builder.add_node("execute", execute, metadata={"execute_in": "step"})
    builder.add_edge(START, "approval")
    builder.add_conditional_edges("approval", lambda state: "execute" if state["approved"] else END)
    builder.add_edge("execute", END)
    app = DBOSLangGraphPlugin("approval_v1", interrupts=True).register_graph(builder)
    DBOS.launch()
    try:
        workflow_id = str(uuid.uuid4())
        with SetWorkflowID(workflow_id):
            handle = await app.start({"action": "Run the example tool"})
        pause = await app.get_interrupt(workflow_id)
        if pause is None:
            raise RuntimeError("The graph did not reach its approval step")
        print(pause.value)
        answer = await asyncio.to_thread(input, "Approve [y/N]? ")
        await app.resume(workflow_id, pause.id, answer.strip().lower() == "y")
        print(await handle.get_result())
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    asyncio.run(main())
