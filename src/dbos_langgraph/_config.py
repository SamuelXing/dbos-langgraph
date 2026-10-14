"""Persistable invocation configuration, separate from LangGraph's live internals."""

from collections.abc import Mapping
from contextvars import ContextVar
from copy import deepcopy
from typing import Any

from ._state import fingerprint

invocation_config: ContextVar[tuple[dict[str, Any], bool] | None] = ContextVar(
    "dbos_langgraph_invocation_config", default=None
)


def normalize_config(
    config: Mapping[str, Any] | None, recursion_limit: int | None
) -> tuple[dict[str, Any], int]:
    if config is not None and not isinstance(config, Mapping):
        raise TypeError("config must be a mapping")
    if recursion_limit is not None and (
        isinstance(recursion_limit, bool)
        or not isinstance(recursion_limit, int)
        or recursion_limit < 1
    ):
        raise ValueError("recursion_limit must be a positive integer")
    result = deepcopy(dict(config or {}))
    if any(type(key) is not str for key in result):
        raise TypeError("config fields must be strings")
    unsupported = set(result) - {"configurable", "tags", "metadata", "run_name", "recursion_limit"}
    if unsupported:
        raise ValueError(f"unsupported config fields: {sorted(unsupported)}")
    limit = result.pop("recursion_limit", 1000 if recursion_limit is None else recursion_limit)
    if recursion_limit is not None and limit != recursion_limit:
        raise ValueError("conflicting recursion_limit values")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("recursion_limit must be a positive integer")
    for field in ("configurable", "metadata"):
        if field in result and type(result[field]) is not dict:
            raise TypeError(f"{field} must be a JSON object")
    if "tags" in result and (
        type(result["tags"]) is not list or any(type(tag) is not str for tag in result["tags"])
    ):
        raise TypeError("tags must be a list of strings")
    if "run_name" in result and type(result["run_name"]) is not str:
        raise TypeError("run_name must be a string")
    fingerprint(result, allow_messages=False)
    if any(key.startswith("__") for key in result.get("configurable", {})):
        raise ValueError("internal configurable keys are unsupported")
    return result, limit
