"""Continuous, relay-independent watchdog for the durable queue outbox.

WHY THIS EXISTS (2026-07-20 incident)
-------------------------------------
``Bus.dispatch`` does not talk to the broker. It commits a row to the
``queue_job_delivery`` ledger and ``queue:relay`` is the ONLY process that
turns those rows into broker messages. When the relay was not running,
1250 jobs accumulated as ``pending`` while every trigger cheerfully
reported "started" and nothing ran. Nothing noticed for hours.

The reason the existing telemetry stayed silent is structural, not a
missing threshold: every outbox gauge is published by
``AMQPDriver.refresh_delivery_metrics()``, which only ever runs inside
the relay/hooks processes. When the relay dies the series simply stop
existing and any rule watching them goes to "no data" — it does not fire.
**The observer was the observed.**

This watchdog samples the outbox from a process with no dependency on
the relay: the app's scheduler. ``schedule:work`` runs configured jobs
INLINE via ``handle()``, never through ``Bus.dispatch`` — essential,
because a stall detector that dispatched itself onto the queue would be
swallowed by the exact outage it exists to report.

DIVISION OF LABOUR
------------------
``PublicationBacklogProbe`` is the STARTUP advisory (one banner when an
operator types ``queue:work`` into a stalled outbox); this class is the
CONTINUOUS alarm. Both read the store's own due predicates and the same
``queue.outbox_stall_*`` knobs, so the two surfaces can never disagree
about what "stalled" means.

SIGNAL CHOICE
-------------
The alarm gate is the AGE of the oldest due row, not a raw count. A
burst of dispatches is normal and healthy; the same rows still
unpublished five minutes later is not. Age is also self-normalising for
idle periods: a stale ``published_at`` is expected overnight, whereas a
due row that is not draining is unambiguously broken regardless of
traffic shape. Last-publish age is reported as diagnostic context in the
alert body.

TERMINAL-HOOK HALF
------------------
The same blind spot exists on the other side of the ledger: a row
reaches a terminal status, commits, and ``queue:hooks`` is the sole
thing that runs its post-hooks. This watchdog samples that half too,
from the same relay-independent process, through the store's single
hook-due predicate (``_HOOK_DUE_FILTER_TEMPLATE``).

PRODUCT SEAM
------------
Gauge publication is the ONE product-owned piece: metric names are
product-prefixed and scraped from the product's scheduler port, and the
alert rules watching them are product infrastructure. Subclass and
override :meth:`publish_metrics`; everything else — sampling, judgement,
alert lifecycle, throttling — is framework behavior.
"""

from __future__ import annotations

import time
from typing import Any

from cara.configuration import config
from cara.facades import Cache, Log
from cara.observability import AlertSink

_ALERT_TITLE = "Queue publication outbox is stalled"
_HOOK_ALERT_TITLE = "Queue terminal-hook outbox is stalled"


class QueueOutboxHealth:
    """Sample, judge and announce durable outbox publication + hook health."""

    #: Paging dedup identity. Stable across ticks on purpose: an ongoing
    #: stall must collapse into ONE incident, not one per minute.
    ALERT_DEDUP_KEY = "queue_outbox_stalled"

    #: Marks that an incident is currently open, so recovery can emit a
    #: matching ``resolved``. Long TTL — it outlives the re-notify window.
    ACTIVE_CACHE_KEY = "queue:outbox:stall:active"

    #: Set with ``Cache.add`` (atomic set-if-absent) for the duration of the
    #: re-notify window. Its presence means "already announced recently";
    #: its expiry is what lets a CONTINUING stall speak up again.
    NOTIFY_CACHE_KEY = "queue:outbox:stall:notified"

    #: Same triple as above, for the terminal-hook half of the outbox. A
    #: separate identity/keyspace is deliberate: an open publication
    #: incident must never throttle the first hooks page, and each half
    #: resolves independently.
    HOOK_ALERT_DEDUP_KEY = "queue_hook_outbox_stalled"
    HOOK_ACTIVE_CACHE_KEY = "queue:outbox:hooks:stall:active"
    HOOK_NOTIFY_CACHE_KEY = "queue:outbox:hooks:stall:notified"

    # ── configuration ────────────────────────────────────────────────
    @staticmethod
    def stall_age_seconds() -> int:
        """How long a due row may wait before it counts as a stall."""
        return max(int(config("queue.outbox_stall_age_seconds", 300)), 1)

    @staticmethod
    def stall_min_pending() -> int:
        """How many aged rows must be present before alarming."""
        return max(int(config("queue.outbox_stall_min_pending", 1)), 1)

    @staticmethod
    def stall_renotify_seconds() -> int:
        """Minimum gap between two announcements of the SAME stall.

        Shared by both halves — one anti-spam cadence for the whole
        outbox watchdog, not a second knob to tune.
        """
        return max(int(config("queue.outbox_stall_renotify_seconds", 900)), 1)

    @staticmethod
    def hook_stall_age_seconds() -> int:
        """How long a hook-due row may wait before it counts as a stall.

        Looser than the publication budget by default: the first hooks
        retry backoff is 60s and terminal-hook delivery is lower-urgency
        than getting a job onto the broker at all, so 600s avoids paging
        on one ordinary retry cycle.
        """
        return max(int(config("queue.hook_outbox_stall_age_seconds", 600)), 1)

    @staticmethod
    def hook_stall_min_pending() -> int:
        """How many aged hook-due rows must be present before alarming."""
        return max(int(config("queue.hook_outbox_stall_min_pending", 1)), 1)

    # ── sampling ─────────────────────────────────────────────────────
    @classmethod
    def sample(cls, store: Any | None = None) -> dict[str, float]:
        """Return one bounded aggregate snapshot of the durable outbox.

        ``store`` is a ``QueueJobDeliveryStore`` (injectable for tests);
        by default it is acquired exactly the way the startup probe does.
        Products that never deploy the ledger simply do not schedule the
        sweep, so a missing table here fails loud rather than guessing.
        """
        return cls._store(store).outbox_health_metrics()

    @staticmethod
    def _store(store: Any | None) -> Any:
        if store is not None:
            return store
        from cara.facades import Queue  # local: cycle with cara.facades

        return Queue.driver("amqp").delivery_store

    # ── judgement ────────────────────────────────────────────────────
    @classmethod
    def is_stalled(cls, snapshot: dict[str, float]) -> bool:
        """True when due rows have aged past the configured budget.

        BOTH conditions must hold. The age gate is what suppresses false
        alarms from ordinary bursts and brief worker slowness; the count
        gate exists so an operator can additionally tolerate a small
        number of chronically slow rows without muting real outages.
        """
        return (
            snapshot["oldest_due_age"] >= cls.stall_age_seconds()
            and snapshot["due_pending"] >= cls.stall_min_pending()
        )

    @classmethod
    def is_hook_stalled(cls, snapshot: dict[str, float]) -> bool:
        """Same two-gate judgement as :meth:`is_stalled`, hooks half."""
        return (
            snapshot["hook_oldest_due_age"] >= cls.hook_stall_age_seconds()
            and snapshot["hook_due_pending"] >= cls.hook_stall_min_pending()
        )

    # ── metrics (product seam) ───────────────────────────────────────
    @classmethod
    def publish_metrics(
        cls, snapshot: dict[str, float], stalled: bool, hook_stalled: bool
    ) -> bool:
        """Mirror the snapshot into product gauges — override per product.

        The base class publishes nothing: gauge names are
        product-prefixed, scraped from the product's scheduler port, and
        watched by product alert rules, so their home is the product
        subclass. Overrides should write a freshness timestamp LAST and
        only after every value landed, so a probe that dies halfway
        strands the stamp instead of freezing at last-known-good values.
        Return ``False`` when any gauge failed to land.
        """
        return True

    # ── announcement ─────────────────────────────────────────────────
    @classmethod
    def announce(cls, snapshot: dict[str, float], stalled: bool) -> str:
        """Emit / suppress / resolve the publication-outbox operator alert.

        Returns ``fired``, ``throttled``, ``resolved`` or ``quiet``.

        Spam policy: one announcement per ``stall_renotify_seconds``
        window for as long as the stall lasts. ``Cache.add`` is the
        atomic gate, so multiple scheduler processes firing the same
        tick still produce exactly one alert. It re-announces rather
        than latching once, because a stall that stays silent after its
        first page is indistinguishable from a stall nobody noticed —
        which is precisely how the incident happened.
        """
        if stalled:
            return cls._announce_stall(
                dedup_key=cls.ALERT_DEDUP_KEY,
                active_key=cls.ACTIVE_CACHE_KEY,
                notify_key=cls.NOTIFY_CACHE_KEY,
                title=_ALERT_TITLE,
                body=cls._stall_body(snapshot),
                context={
                    "due_pending": int(snapshot["due_pending"]),
                    "oldest_due_age_seconds": int(snapshot["oldest_due_age"]),
                    "last_publish_age_seconds": int(snapshot["last_publish_age"]),
                    "stall_age_budget_seconds": cls.stall_age_seconds(),
                },
                category="queue.outbox.stall",
            )
        return cls._announce_recovery(
            dedup_key=cls.ALERT_DEDUP_KEY,
            active_key=cls.ACTIVE_CACHE_KEY,
            notify_key=cls.NOTIFY_CACHE_KEY,
            title=_ALERT_TITLE,
            body=(
                "Outbox publication is draining again; the oldest due row is "
                f"{int(snapshot['oldest_due_age'])}s old."
            ),
            log_args=(int(snapshot["due_pending"]), int(snapshot["oldest_due_age"])),
            category="queue.outbox.stall",
        )

    @classmethod
    def announce_hooks(cls, snapshot: dict[str, float], hook_stalled: bool) -> str:
        """Emit / suppress / resolve the terminal-hook-outbox operator alert.

        Independent identity from :meth:`announce` on purpose: an open
        publication incident must not throttle the first hooks page, and
        a hooks recovery must not resolve a publication incident (or
        vice versa). Same throttle cadence (``stall_renotify_seconds``)
        as the publication half — one anti-spam knob for the whole
        watchdog.
        """
        if hook_stalled:
            return cls._announce_stall(
                dedup_key=cls.HOOK_ALERT_DEDUP_KEY,
                active_key=cls.HOOK_ACTIVE_CACHE_KEY,
                notify_key=cls.HOOK_NOTIFY_CACHE_KEY,
                title=_HOOK_ALERT_TITLE,
                body=cls._hook_stall_body(snapshot),
                context={
                    "hook_due_pending": int(snapshot["hook_due_pending"]),
                    "hook_oldest_due_age_seconds": int(
                        snapshot["hook_oldest_due_age"]
                    ),
                    "hook_stall_age_budget_seconds": cls.hook_stall_age_seconds(),
                },
                category="queue.outbox.hook_stall",
            )
        return cls._announce_recovery(
            dedup_key=cls.HOOK_ALERT_DEDUP_KEY,
            active_key=cls.HOOK_ACTIVE_CACHE_KEY,
            notify_key=cls.HOOK_NOTIFY_CACHE_KEY,
            title=_HOOK_ALERT_TITLE,
            body=(
                "Terminal-hook delivery is draining again; the oldest due "
                f"row is {int(snapshot['hook_oldest_due_age'])}s old."
            ),
            log_args=(
                int(snapshot["hook_due_pending"]),
                int(snapshot["hook_oldest_due_age"]),
            ),
            category="queue.outbox.hook_stall",
        )

    @classmethod
    def _stall_body(cls, snapshot: dict[str, float]) -> str:
        return (
            f"{int(snapshot['due_pending'])} job(s) are committed to the "
            "queue publication outbox and due for publication; the oldest "
            f"has been waiting {int(snapshot['oldest_due_age'])}s (budget "
            f"{cls.stall_age_seconds()}s). "
            + (
                "Nothing has ever been published from this ledger."
                if snapshot["last_publish_age"] < 0
                else "Last successful publication was "
                f"{int(snapshot['last_publish_age'])}s ago."
            )
            + " Dispatch reports success while the work never reaches "
            "the broker. Check that `craft queue:relay` is running."
        )

    @classmethod
    def _hook_stall_body(cls, snapshot: dict[str, float]) -> str:
        return (
            f"{int(snapshot['hook_due_pending'])} job(s) in the delivery "
            "ledger reached a terminal status and are claimable for "
            "post-hook delivery; the oldest has been waiting "
            f"{int(snapshot['hook_oldest_due_age'])}s (budget "
            f"{cls.hook_stall_age_seconds()}s). A row counts as due once "
            "post_hooks_completed_at and post_hooks_quarantined_at are "
            "both still NULL and its post_hooks lease has expired or was "
            "never taken. Check that `craft queue:hooks` is running."
        )

    @classmethod
    def _announce_stall(
        cls,
        *,
        dedup_key: str,
        active_key: str,
        notify_key: str,
        title: str,
        body: str,
        context: dict[str, Any],
        category: str,
    ) -> str:
        # Always logged, every tick, regardless of throttling: the log is
        # the durable forensic trail and must not have holes. Only the
        # paging channel is rate limited.
        Log.error("%s: %s", title, body, category=category)

        # Cache is best-effort infrastructure. If it is unreachable we
        # would rather page twice than not at all, so failures fall
        # through to "announce".
        first_or_due = True
        try:
            first_or_due = bool(
                Cache.add(
                    notify_key,
                    str(int(time.time())),
                    cls.stall_renotify_seconds(),
                )
            )
            Cache.put(
                active_key,
                str(int(time.time())),
                cls.stall_renotify_seconds() * 4,
            )
        except Exception as exc:  # noqa: BLE001 — never mute an outage
            Log.warning(
                "Outbox stall alert throttle unavailable; announcing unthrottled: %s",
                exc,
                category=category,
            )

        if not first_or_due:
            return "throttled"

        AlertSink.fire(
            severity="critical",
            title=title,
            body=body,
            dedup_key=dedup_key,
            context=context,
        )
        return "fired"

    @classmethod
    def _announce_recovery(
        cls,
        *,
        dedup_key: str,
        active_key: str,
        notify_key: str,
        title: str,
        body: str,
        log_args: tuple,
        category: str,
    ) -> str:
        try:
            was_active = bool(Cache.get(active_key))
        except Exception:  # noqa: BLE001 — a cache miss must not page
            return "quiet"
        if not was_active:
            return "quiet"

        try:
            Cache.forget(active_key)
            Cache.forget(notify_key)
        except Exception as exc:  # noqa: BLE001
            # Fail-open by design: the recovery notice below is the point
            # of this branch, and losing a best-effort cache eviction must
            # not suppress it. Both keys carry TTLs, so a failed forget
            # self-heals; the worst case is one redundant page later.
            # Logged rather than swallowed — a cache fault here is
            # invisible otherwise, and this probe is the relay's only
            # observer.
            Log.warning(
                "Queue outbox recovery could not evict its alert-state keys: %s",
                exc,
                category=category,
            )

        Log.info(
            "%s recovered: %s due row(s), oldest %ss.",
            title,
            log_args[0],
            log_args[1],
            category=category,
        )
        AlertSink.fire(
            severity="resolved",
            title=title,
            body=body,
            dedup_key=dedup_key,
        )
        return "resolved"

    # ── orchestration ────────────────────────────────────────────────
    @classmethod
    def observe(cls, store: Any | None = None) -> dict[str, Any]:
        """Full probe: sample → gauges → alert both halves. Returns outcomes."""
        snapshot = cls.sample(store)
        stalled = cls.is_stalled(snapshot)
        hook_stalled = cls.is_hook_stalled(snapshot)
        cls.publish_metrics(snapshot, stalled, hook_stalled)
        outcome = cls.announce(snapshot, stalled)
        hook_outcome = cls.announce_hooks(snapshot, hook_stalled)
        return {
            "snapshot": snapshot,
            "stalled": stalled,
            "hook_stalled": hook_stalled,
            "outcome": outcome,
            "hook_outcome": hook_outcome,
        }


__all__ = ["QueueOutboxHealth"]
