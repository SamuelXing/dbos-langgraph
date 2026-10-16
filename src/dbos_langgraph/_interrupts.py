"""Native interrupt state reconstructed from persisted node arguments.

This bridge is tied to the pinned LangGraph scratchpad contract. It never stores
live scratchpads or checkpoints in DBOS; node input carries the resume values.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from itertools import count
from typing import Any, cast

from dbos import DBOS
from dbos._error import DBOSWorkflowCancelledError
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langgraph._internal._constants import (
    CONFIG_KEY_CHECKPOINT_NS,
    CONFIG_KEY_SCRATCHPAD,
    CONFIG_KEY_SEND,
    CONFIG_KEY_TASK_ID,
)
from langgraph._internal._scratchpad import PregelScratchpad
from langgraph.config import get_config
from langgraph.types import Interrupt

from ._state import fingerprint

EVENT_KEY = "dbos_langgraph.interrupt"
RESUME_TOPIC = "dbos_langgraph.resume"
RECEIVE_TIMEOUT_SECONDS = 1


def resume_topic(interrupt_id: str) -> str:
    return f"{RESUME_TOPIC}.{interrupt_id}"


resume_target: ContextVar[tuple[str, list[Any]] | None] = ContextVar(
    "dbos_langgraph_resume_target", default=None
)


@dataclass(frozen=True)
class PausedNode:
    interrupts: tuple[Interrupt[Any], ...]


def node_resume_values() -> list[Any] | None:
    target = resume_target.get()
    if target is None:
        return None
    task_id = get_config()["configurable"][CONFIG_KEY_TASK_ID]
    return target[1] if target[0] == task_id else None


@contextmanager
def interrupt_scope(namespace: str, resumes: list[Any]) -> Iterator[None]:
    scratchpad = PregelScratchpad(
        step=0,
        stop=1,
        call_counter=count().__next__,
        interrupt_counter=count().__next__,
        get_null_resume=lambda consume: None,
        resume=list(resumes),
        subgraph_counter=count().__next__,
    )
    existing = var_child_runnable_config.get() or {}
    configurable = {
        **existing.get("configurable", {}),
        CONFIG_KEY_CHECKPOINT_NS: namespace,
        CONFIG_KEY_SCRATCHPAD: scratchpad,
        # Resume values are already in durable node input, rather than live
        # checkpoint writes. The outer graph receives the node's final update.
        CONFIG_KEY_SEND: lambda writes: None,
    }
    token = var_child_runnable_config.set(
        cast(RunnableConfig, {**existing, "configurable": configurable})
    )
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)


async def _check_running() -> None:
    workflow_id = str(DBOS.workflow_id)
    status = await DBOS.get_workflow_status_async(workflow_id)
    if status is None or status.status not in ("PENDING", "ENQUEUED"):
        raise DBOSWorkflowCancelledError(workflow_id)


async def wait_for_resume(interrupt_id: str) -> Any:
    """Wait durably without retaining a cancelled root for a long receive timeout."""
    while True:
        await _check_running()
        message = await DBOS.recv_async(
            resume_topic(interrupt_id), timeout_seconds=RECEIVE_TIMEOUT_SECONDS
        )
        # Even the correct topic can contain malformed external messages. Ignore
        # those entries so a subsequent valid approval can still reach the graph.
        if not isinstance(message, dict) or message.get("interrupt_id") != interrupt_id:
            continue
        if "value" not in message:
            continue
        try:
            fingerprint(message["value"], allow_messages=False)
        except (TypeError, ValueError):
            continue
        await _check_running()
        return message["value"]
