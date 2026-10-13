"""Options for the single DBOS step used by a leaf node."""

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class StepOptions:
    """Retries count total attempts. Cancellation requires preemptible=True."""

    max_attempts: int = 1
    interval_seconds: float = 1.0
    backoff_rate: float = 2.0
    preemptible: bool = False
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        for name in ("interval_seconds", "backoff_rate", "timeout_seconds"):
            value = getattr(self, name)
            if name == "timeout_seconds" and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
        if type(self.preemptible) is not bool:
            raise ValueError("preemptible must be a boolean")
