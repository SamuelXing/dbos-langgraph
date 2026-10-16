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
from dbos_langgraph._interrupts import EVENT_KEY, RESUME_TOPIC, resume_topic


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
    if args.scenario == "freeze":
        # The plugin must not compile its retained builder again during invocation.
        builder.edges.clear()
        original_compile = StateGraph.compile

        def forbid_compile(*args, **kwargs):
            raise AssertionError("registered graphs must not recompile")

        StateGraph.compile = forbid_compile
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
    publishing = asyncio.Event()
    release_publication = asyncio.Event()
    original_set_event = DBOS.set_event_async
    if args.scenario == "early":

        async def hold_publication(key, value, **kwargs):
            if key == EVENT_KEY and value.get("status") == "paused":
                (directory / "early-id").write_text(value["interrupt"].id)
                publishing.set()
                await release_publication.wait()
            return await original_set_event(key, value, **kwargs)

        DBOS.set_event_async = hold_publication
    DBOS.launch()
    try:
        if args.mode in ("start", "live", "cancel", "unsupported"):
            with SetWorkflowID(args.id):
                handle = await app.start({})
        else:
            handle = await DBOS.retrieve_workflow_async(args.id)
        if args.scenario == "isolation":
            other_id = args.id + "-other"
            with SetWorkflowID(other_id):
                other = await app.start({})
            other_pause = await app.get_interrupt(other_id, timeout_seconds=10)
            assert other_pause is not None
            await app.resume(other_id, other_pause.id, False)
            other_result = await asyncio.wait_for(other.get_result(), 10)
            assert other_result["decision"] is False
            assert "tool_result" not in other_result
            original_pause = await app.get_interrupt(args.id, timeout_seconds=10)
            assert original_pause is not None and original_pause.id != other_pause.id
        if args.scenario == "early":
            await asyncio.wait_for(publishing.wait(), 10)
            early_id = (directory / "early-id").read_text()
            try:
                await app.resume(args.id, early_id, True)
            except ValueError:
                pass
            else:
                raise AssertionError("unpublished pauses must reject approval")
            release_publication.set()
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
            if args.scenario == "message":
                await DBOS.send_async(
                    args.id, {"interrupt_id": "wrong", "value": False}, RESUME_TOPIC
                )
                await DBOS.send_async(
                    args.id, {"interrupt_id": "wrong", "value": False}, resume_topic("wrong")
                )
                await DBOS.send_async(args.id, {"interrupt_id": "wrong"}, resume_topic(pause.id))
                await DBOS.send_async(args.id, {"interrupt_id": pause.id}, resume_topic(pause.id))
            if args.scenario == "cancel-race":
                original_send = DBOS.send_async

                async def cancel_before_send(*send_args, **send_kwargs):
                    await DBOS.cancel_workflow_async(args.id, cancel_children=True)
                    await original_send(*send_args, **send_kwargs)

                DBOS.send_async = cancel_before_send
                try:
                    await app.resume(args.id, pause.id, True)
                finally:
                    DBOS.send_async = original_send
                try:
                    await asyncio.wait_for(handle.get_result(), 5)
                except Exception as exc:
                    assert type(exc).__name__ == "DBOSAwaitedWorkflowCancelledError"
                else:
                    raise AssertionError("cancellation must prevent graph completion")
                print(json.dumps({"cancelled": True}), flush=True)
                return
            if args.mode == "cancel":
                # Keep references before cancellation; a polling handle alone
                # says nothing about whether the actual root task has exited.
                from dbos._dbos import _get_dbos_instance

                tasks = set(_get_dbos_instance()._workflow_tasks)
                await DBOS.cancel_workflow_async(args.id, cancel_children=True)
                try:
                    await handle.get_result()
                except Exception as exc:
                    assert type(exc).__name__ == "DBOSAwaitedWorkflowCancelledError"

                async def wait_for_exit():
                    while any(not task.done() for task in tasks):
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(wait_for_exit(), 5)
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
        DBOS.set_event_async = original_set_event
        if args.scenario == "freeze":
            StateGraph.compile = original_compile
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
        "--scenario",
        choices=[
            "normal",
            "second",
            "workflow",
            "sync",
            "parallel",
            "early",
            "message",
            "freeze",
            "cancel-race",
            "isolation",
        ],
        default="normal",
    )
    asyncio.run(run(parser.parse_args()))
