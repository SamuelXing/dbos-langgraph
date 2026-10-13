"""Canonical fingerprints for the supported JSON state schema."""

import hashlib
import json
from typing import Any


def fingerprint(value: Any) -> str:
    def validate(item: Any) -> None:
        if item is None or type(item) in (str, bool, int, float):
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict and all(type(key) is str for key in item):
            for child in item.values():
                validate(child)
            return
        raise TypeError("durable state must contain only JSON values with string object keys")

    validate(value)
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:
        raise TypeError("durable state must contain finite JSON values") from exc
    return hashlib.sha256(encoded.encode()).hexdigest()
