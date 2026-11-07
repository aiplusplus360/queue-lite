"""Retry delay, as a function you can unit test.

Exponential backoff without jitter synchronises every failed job in a batch onto
the same retry instant, which turns one upstream outage into a thundering herd
of retries at the exact moment the upstream comes back. The jitter here is
multiplicative and bounded, so the delay is predictable enough to reason about
and still spread out.
"""

from __future__ import annotations

import random
from typing import Optional

__all__ = ["compute_delay"]

MAX_EXPONENT = 62  # 2**62 is the last power of two a float represents exactly


def compute_delay(
    attempts: int,
    *,
    base: float = 2.0,
    cap: float = 300.0,
    jitter: float = 0.2,
    rng: Optional[random.Random] = None,
) -> float:
    """Seconds to wait before retry number ``attempts`` (1-based).

    >>> compute_delay(1, jitter=0)
    2.0
    >>> compute_delay(4, jitter=0)
    16.0
    >>> compute_delay(40, jitter=0)
    300.0

    With 20% jitter the result stays inside ``[0.8x, 1.2x]`` of the raw delay:

    >>> raw = compute_delay(3, jitter=0)
    >>> 0.8 * raw <= compute_delay(3, jitter=0.2, rng=random.Random(7)) <= 1.2 * raw
    True
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    if base <= 0:
        raise ValueError("base must be > 0")
    if cap <= 0:
        raise ValueError("cap must be > 0")
    if not 0.0 <= jitter < 1.0:
        raise ValueError("jitter must be in [0, 1)")

    exponent = min(attempts - 1, MAX_EXPONENT)
    raw = min(cap, base * (2.0 ** exponent))
    if jitter == 0.0:
        return raw
    generator = rng or random
    factor = 1.0 - jitter + (2.0 * jitter * generator.random())
    return min(cap, raw * factor)
