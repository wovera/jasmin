from datetime import timedelta

from twisted.internet import defer, reactor


def throttle_delay(throughput, last_at, now, count=1):
    """Seconds to pause so submissions stay within throughput per second; 0 if already at or under rate.
    Reads the full duration (total_seconds), so sub-1/s throughput and multi-second gaps are correct.
    count scales the interval for a single dequeue that carries several wire PDUs (a concatenated SMS)."""
    if throughput <= 0:
        return 0
    interval = timedelta(seconds=count / float(throughput))
    elapsed = now - last_at
    if elapsed >= interval:
        return 0
    return (interval - elapsed).total_seconds()


@defer.inlineCallbacks
def slow_down(seconds):
    # Block on waitDeferred for 'seconds'
    waitDeferred = defer.Deferred()
    reactor.callLater(seconds, waitDeferred.callback, None)
    yield waitDeferred


class LearnedRate:
    """Additive-increase, multiplicative-decrease around a contract ceiling; a carrier's throttle is the only
    truthful capacity signal SMPP 3.4 gives."""

    def __init__(self, ceiling):
        self.ceiling = float(ceiling)
        self.current = float(ceiling)
        self._last_clean_at = None

    def throttled(self):
        self.current = max(1.0, self.current / 2)
        self._last_clean_at = None

    def clean(self, now):
        if self._last_clean_at is None:
            self._last_clean_at = now
            return
        elapsed = (now - self._last_clean_at).total_seconds()
        if elapsed >= 1:
            self.current = min(self.ceiling, self.current + int(elapsed))
            self._last_clean_at = now
