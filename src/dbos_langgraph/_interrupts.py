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

EVENT_KEY = "dbos_langgraph.interrupt"
RESUME_TOPIC = "dbos_langgraph.resume"
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
