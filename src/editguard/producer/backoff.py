"""Exponential backoff with full jitter, capped (design: reconnect capped at 60 s)."""

import random


def backoff_delay(attempt: int, base_s: float = 1.0, cap_s: float = 60.0) -> float:
    """Seconds to wait before reconnect attempt number `attempt` (0 = first retry)."""
    ceiling = min(cap_s, base_s * 2**attempt)
    return random.uniform(0, ceiling)  # noqa: S311 - jitter spreads retries; not security
