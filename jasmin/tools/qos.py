from datetime import timedelta

from twisted.internet import defer, reactor


def throttle_delay(throughput, last_at, now):
    """Seconds to pause so submissions stay within throughput per second; 0 if already at or under rate.
    Reads the full duration (total_seconds), so sub-1/s throughput and multi-second gaps are correct."""
    if throughput <= 0:
        return 0
    interval = timedelta(seconds=1 / float(throughput))
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
