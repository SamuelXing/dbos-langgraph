"""Canonical state identities and stable IDs for LangChain messages."""

import hashlib
import json
import uuid
from copy import deepcopy
from typing import Any

from langchain_core.messages import BaseMessage, messages_from_dict, messages_to_dict


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:
        raise TypeError("durable state must contain finite JSON values") from exc


def fingerprint(value: Any, *, allow_messages: bool = True) -> str:
    has_messages = False

    def encode(item: Any) -> Any:
        nonlocal has_messages
        if item is None or type(item) in (str, bool, int, float):
            return ["value", item]
        if type(item) is list:
            return ["list", [encode(child) for child in item]]
        if type(item) is dict and all(type(key) is str for key in item):
            return ["dict", [[key, encode(item[key])] for key in sorted(item)]]
        if allow_messages and isinstance(item, BaseMessage):
            fingerprint(item.__dict__, allow_messages=False)
            if item.model_extra:
                fingerprint(item.model_extra, allow_messages=False)
            payload = messages_to_dict([item])[0]
            reconstructed = messages_from_dict([payload])[0]
            if type(reconstructed) is not type(item):
                raise TypeError("custom message subclasses are unsupported")
            # Message metadata must itself be JSON; arbitrary provider objects
            # cannot form a stable identity across worker environments.
            fingerprint(payload, allow_messages=False)
            has_messages = True
            return ["message", encode(payload)]
        raise TypeError("durable state must contain JSON values or supported LangChain messages")

    encoded = encode(value)
    # Preserve existing JSON-only histories. Typed message trees use a separate
    # hash domain so user-supplied JSON cannot impersonate a message object.
    canonical = "langchain-messages-v1:" + _json(encoded) if has_messages else _json(value)
    return hashlib.sha256(canonical.encode()).hexdigest()


def prepare_messages(value: Any, *, seed: str | None = None) -> Any:
    """Copy state and assign missing message IDs before durable results are stored.

    Root input IDs are generated before starting its workflow. Node update IDs
    derive from the child workflow ID and field path, including workflow-mode nodes.
    """
    result = deepcopy(value)

    def visit(item: Any, path: list[str | int]) -> None:
        if isinstance(item, BaseMessage):
            if item.id is None:
                item.id = (
                    str(uuid.uuid4())
                    if seed is None
                    else str(uuid.uuid5(uuid.NAMESPACE_URL, seed + ":" + _json(path)))
                )
        elif type(item) is list:
            for index, child in enumerate(item):
                visit(child, [*path, index])
        elif type(item) is dict:
            for key, child in item.items():
                visit(child, [*path, key])

    visit(result, [])
    fingerprint(result)
    return result
