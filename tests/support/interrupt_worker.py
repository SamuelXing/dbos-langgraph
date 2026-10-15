"""Hold native interrupt execution at durable crash boundaries."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, TypedDict

from dbos import DBOS, SetWorkflowID
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from dbos_langgraph import DBOSLangGraphPlugin


class State(TypedDict, total=False):
    prompt: str
    decision: Any
    tool_result: int
    done: bool


async def run(args):
    directory = Path(args.directory)

    def audit(name):
        with (directory / "calls").open("a") as stream:
            stream.write(name + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def gate(name):
        (directory / name).touch()
        while not (directory / "release").exists():
            await asyncio.sleep(0.01)

    async def model(state):
        audit("model")
        return {"prompt": "Run the tool?"}

    async def approve(state):
        audit("approval-entered")
        decision = interrupt({"question": state["prompt"]})
        if args.scenario == "second":
            interrupt("A second approval")
        audit("approval-consumed")
        if args.boundary == "approval":
            await gate("approval-ready")
        return {"decision": decision}

    async def tool(state):
        audit("tool")
        return {"tool_result": 42}

    async def tail(state):
        audit("tail")
        if args.boundary == "tool":
            await gate("tail-ready")
        return {"done": True}

    def approve_sync(state):
        audit("approval-entered")
        return {"decision": interrupt({"question": state["prompt"]})}

    if args.scenario == "sync":
        approve = approve_sync
    builder = StateGraph(State)
    for name, function in (("model", model), ("approve", approve), ("tool", tool), ("tail", tail)):
        mode = "workflow" if name == "approve" and args.scenario == "workflow" else "step"
        builder.add_node(name, function, metadata={"execute_in": mode})
    builder.add_edge(START, "model")
    builder.add_edge("model", "approve")
    if args.scenario == "parallel":
        builder.add_node("other_approval", approve, metadata={"execute_in": "step"})
        builder.add_edge("model", "other_approval")
        builder.add_edge("other_approval", END)
    builder.add_conditional_edges(
        "approve", lambda state: "tool" if state["decision"] else "tail", ["tool", "tail"]
    )
    builder.add_edge("tool", "tail")
    builder.add_edge("tail", END)
    app = DBOSLangGraphPlugin("interrupt-example", interrupts=True).register_graph(builder)
    DBOS(
        config={
            "name": "interrupt-tests",
            "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
            "executor_id": args.id,
            "application_version": args.id,
            "enable_otlp": False,
            "log_level": "CRITICAL",
        }
    )
    DBOS.launch()
    try:
        if args.mode in ("start", "live", "cancel", "unsupported"):
            with SetWorkflowID(args.id):
                handle = await app.start({})
        else:
            handle = await DBOS.retrieve_workflow_async(args.id)
        if args.mode == "unsupported":
            if args.scenario == "second":
                pause = await app.get_interrupt(args.id, timeout_seconds=10)
                assert pause is not None
                await app.resume(args.id, pause.id, True)
            try:
                await asyncio.wait_for(handle.get_result(), 20)
            except NotImplementedError as exc:
                print(json.dumps({"unsupported": str(exc)}), flush=True)
                return
            raise AssertionError("unsupported interrupts must fail")
        if args.mode in ("start", "live", "resume", "cancel"):
            pause = await app.get_interrupt(args.id, timeout_seconds=10)
            assert pause is not None
            (directory / "pause-id").write_text(pause.id)
            (directory / "paused").touch()
            if args.mode == "start":
                await asyncio.Event().wait()
            if args.mode == "cancel":
                await DBOS.cancel_workflow_async(args.id, cancel_children=True)
                try:
                    await handle.get_result()
                except Exception as exc:
                    assert type(exc).__name__ == "DBOSAwaitedWorkflowCancelledError"
                assert await app.get_interrupt(args.id, timeout_seconds=0) is None
                try:
                    await app.resume(args.id, pause.id, True)
                except ValueError:
                    print(json.dumps({"cancelled": True}), flush=True)
                    return
                raise AssertionError("cancelled roots cannot be resumed")
            try:
                await app.resume(args.id, "stale-interrupt", True)
            except ValueError:
                pass
            else:
                raise AssertionError("stale resume must be rejected")
            await app.resume(args.id, pause.id, json.loads(args.response))
            # A conflicting retry has the same immutable request identity.
            await app.resume(args.id, pause.id, "conflicting retry")
        result = await asyncio.wait_for(handle.get_result(), 20)
        if args.mode in ("live", "resume"):
            await app.resume(args.id, pause.id, "retry after completion")
        assert await app.get_interrupt(args.id, timeout_seconds=0) is None
        print(json.dumps(result), flush=True)
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", choices=["start", "resume", "recover", "live", "cancel", "unsupported"]
    )
    parser.add_argument("--id", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--boundary", choices=["pause", "approval", "tool"], default="pause")
    parser.add_argument("--response", default="true")
    parser.add_argument(
        "--scenario", choices=["normal", "second", "workflow", "sync", "parallel"], default="normal"
    )
    asyncio.run(run(parser.parse_args()))
