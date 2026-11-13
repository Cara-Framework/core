"""CoalescingJob contract — a burst of the same ask runs once.

A job whose run reads the world as it is when it STARTS (a marketplace read a
change notification asks for) answers every ask made before that start. Its
dispatch therefore folds into a delivery of the same ``coalesce_id()`` that
is still waiting to start, and returns that delivery's id: 500 notifications
for one product become one read, not 500 (WooCommerce webhook storm,
2026-11-12).

Unlike :class:`UniqueJob` the fence covers only deliveries that have NOT
started. A running delivery may have read before the change this ask
announces, so an ask that arrives while one runs queues one more delivery —
and every later ask folds into that one. A delivery waits at most
``coalesce_within_seconds`` longer than its own due time for the one it
folds into, so a back-off far in the future never swallows a fresh ask.

The fold alone made a burst about one read per read-time: the delivery
queued behind a running one became due the moment the run ended (Woo storm,
2026-11-13: 301 notifications in 4.2 s, 91 reads of ~0.1-0.5 s). With
``coalesce_delay_seconds`` that delivery is due that long after the dispatch
that queued it, so the rest of the burst folds into it: a burst is about one
read per delay, and a lone ask, with nothing of its key running, still starts
at once.

The fold is an optimisation, not a correctness fence: the ledger takes a
transaction-scoped advisory lock on the key, and a dispatch that cannot fold
simply queues its own delivery. A delayed dispatch (``later``/``schedule``,
a retry) records its key, so later asks can fold into it, but never folds
itself: its due time and its retry settlement are its own.

Usage:
    class PullProductJob(ShouldQueue, Queueable, CoalescingJob):
        def coalesce_id(self) -> str:
            return f"pull-product:{self.channel_id}:{self.product_id}"
"""

from __future__ import annotations


class CoalescingJob:
    """Mixin for jobs whose waiting deliveries absorb repeated dispatches."""

    #: How much later than its own due time a dispatch may wait for the
    #: waiting delivery it folds into.
    coalesce_within_seconds: int = 60

    #: The debounce: a delivery queued while one of its key RUNS is due this
    #: many seconds later instead of at once, so the asks of a burst fold into
    #: it rather than into one read per read-time. A lone ask (nothing of the
    #: key running) still starts at once. 0 turns it off.
    coalesce_delay_seconds: int = 0

    def coalesce_id(self) -> str:
        """The key a waiting delivery answers for; overridden by every job."""
        raise NotImplementedError("CoalescingJob must implement coalesce_id()")
