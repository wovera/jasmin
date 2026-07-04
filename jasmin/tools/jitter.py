"""Bounded jitter for retry/reconnect delays."""

import random


def jittered(delay, factor=0.25):
    """Spread a fixed delay by +/- factor so concurrent connectors don't reconnect or requeue in lockstep
    and stampede the peer when it recovers."""
    if not delay:
        return delay
    return delay * (1 + random.uniform(-factor, factor))
