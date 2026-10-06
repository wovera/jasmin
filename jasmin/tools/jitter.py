"""Bounded jitter for retry/reconnect delays."""

import random


def jittered(delay, factor=0.25):
    """Spread a fixed delay by +/- factor so concurrent connectors don't reconnect or requeue in lockstep
    and stampede the peer when it recovers."""
    if not delay:
        return delay
    return delay * (1 + random.uniform(-factor, factor))


BACKOFF_CEILING_SECONDS = 300
BACKOFF_FLOOR_SECONDS = 1


def backoff(delay, attempt, ceiling=BACKOFF_CEILING_SECONDS):
    """The jittered delay before retry number attempt (0 for the first), doubling from delay - at least one second - up
    to ceiling, so a peer that keeps refusing is retried a bounded number of times per hour instead of at a fixed pace
    forever."""
    delay = max(delay, BACKOFF_FLOOR_SECONDS)
    return jittered(max(delay, min(ceiling, delay * 2 ** min(max(attempt, 0), 16))))
