"""A lease that lives exactly as long as the run holding it.

A lease sized to a job's worst case (timeout + grace) outlives a dead worker
by that whole budget. Live, 2026-10-30: a worker restart mid-pull left the
order poll's ``job_lock:`` lease (3600 + 300 s), its provider concurrency
slot and its ``queue_job_delivery`` execution lease held for about an hour;
every later poll of that channel waited on a lease nobody owned, and the
orphaned delivery row held a slot of its queue's broker window the whole
time.

A SHORT lease renewed by the process that holds it inverts that: a run of any
length keeps its lease, and a dead process's lease lapses within one lease.
Renewal happens :attr:`LeaseHeartbeat.RENEWALS_PER_LEASE` times per lease, so
a renewal can miss twice before the lease is at risk.

THE HEARTBEAT IS A DAEMON THREAD, NOT AN ASYNCIO TASK. Jobs block their event
loop on the synchronous ORM and on synchronous service calls for minutes at a
time (an order pull is one sync call); a task-based heartbeat would starve
behind them and drop a live run's lease. A daemon thread dies with its
process, which is exactly the lifetime a lease must have.

Every renewal runs in a FRESH :class:`contextvars.Context`. The ORM keeps its
open-transaction registry in a ContextVar holding a shared dict, so a
renewal that inherited the run's context would write inside the run's
transaction — and be rolled back with it.

``renew`` must be a compare-and-set on the holder's token that returns
``False`` once the lease is no longer this holder's. The heartbeat then stops
for good; it never re-takes a lease, because that would resurrect one another
run may already hold.

``release`` (optional) hands the lease back without finishing the run. Only
:meth:`LeaseHeartbeat.release_all` calls it — the worker's last step before a
forced exit, for runs that ignored cancellation — so a restart frees the
lease at once instead of one lease later.
"""

from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from typing import ClassVar

from cara.facades import Log

__all__ = ["LeaseHeartbeat"]


class LeaseHeartbeat:
    """Renew one lease from a daemon thread until the run stops it."""

    #: Renewals per lease length: a lease survives two missed renewals.
    RENEWALS_PER_LEASE = 3

    _live: ClassVar[set[LeaseHeartbeat]] = set()
    _live_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        name: str,
        *,
        lease_seconds: float,
        renew: Callable[[], bool],
        release: Callable[[], None] | None = None,
    ) -> None:
        if isinstance(lease_seconds, bool) or not lease_seconds > 0:
            raise ValueError("lease_seconds must be positive")
        self.name = name
        self.lease_seconds = float(lease_seconds)
        self.interval_seconds = self.lease_seconds / self.RENEWALS_PER_LEASE
        self._renew = renew
        self._release = release
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None
        self.renewals = 0
        self.lost = False

    def start(self) -> LeaseHeartbeat:
        with self._live_lock:
            self._live.add(self)
        thread = threading.Thread(
            target=self._run,
            name=f"lease-heartbeat:{self.name}",
            daemon=True,
        )
        self._thread = thread
        thread.start()
        return self

    def stop(self) -> None:
        """Stop renewing. Idempotent; never blocks on an in-flight renewal.

        A renewal still in flight lands on a released lease and finds it gone
        (the compare-and-set misses); it can never extend or recreate it.
        """
        self._stopped.set()
        with self._live_lock:
            self._live.discard(self)

    @property
    def stopped(self) -> bool:
        return self._stopped.is_set()

    def _run(self) -> None:
        while not self._stopped.wait(self.interval_seconds):
            try:
                renewed = contextvars.Context().run(self._renew)
            except Exception as exc:
                # A cache or database blip: the lease still has two renewals
                # of headroom, so keep trying on the normal cadence.
                Log.warning(
                    "Lease %s renewal failed; retrying in %.0fs: %s",
                    self.name,
                    self.interval_seconds,
                    exc,
                    category="cara.queue.lease",
                )
                continue
            if self._stopped.is_set():
                return
            if not renewed:
                self.lost = True
                self.stop()
                Log.warning(
                    "Lease %s is no longer held by this run; renewal stopped.",
                    self.name,
                    category="cara.queue.lease",
                )
                return
            self.renewals += 1

    @classmethod
    def live(cls) -> list[LeaseHeartbeat]:
        with cls._live_lock:
            return list(cls._live)

    @classmethod
    def release_all(cls, *, timeout_seconds: float = 5.0) -> int:
        """Stop and release every lease this process still holds.

        The worker's last step before a forced exit: the runs still holding
        a lease ignored cancellation and die with the process, so their
        leases are handed back now instead of lapsing one lease later. Runs
        on a helper thread joined against ``timeout_seconds`` so an
        unreachable cache or database can never hold the exit hostage.
        Returns the number of leases released.
        """
        heartbeats = cls.live()
        for heartbeat in heartbeats:
            heartbeat.stop()
        releasable = [hb for hb in heartbeats if hb._release is not None]
        if not releasable:
            return 0
        released: list[str] = []

        def _release_each() -> None:
            for heartbeat in releasable:
                try:
                    contextvars.Context().run(heartbeat._release)
                except Exception as exc:
                    Log.warning(
                        "Lease %s could not be released before exit; it lapses "
                        "within %.0fs: %s",
                        heartbeat.name,
                        heartbeat.lease_seconds,
                        exc,
                        category="cara.queue.lease",
                    )
                else:
                    released.append(heartbeat.name)

        helper = threading.Thread(
            target=_release_each,
            name="lease-release-before-exit",
            daemon=True,
        )
        helper.start()
        helper.join(max(0.0, float(timeout_seconds)))
        return len(released)
