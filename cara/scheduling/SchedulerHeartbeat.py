"""The scheduler's heartbeat — who notices when the clock itself stops.

Every recurring job runs off ONE process, ``schedule:work``. When it dies,
the watchdogs meant to catch stale work are themselves scheduled jobs, so
they fall silent with it. The check therefore must NOT run on the scheduler:

* the product schedules a tiny INLINE job (every minute) that calls
  :func:`record_scheduler_tick` — an executed job, not just a live keep-alive
  loop, so a wedged executor stops the tick too;
* a process that is still alive (the API's health/status read) calls
  :func:`scheduler_status`. No tick for ``stale_after_seconds`` (default
  :data:`STALE_AFTER_SECONDS`) is ``delayed`` and, when the caller names an
  ``alert_family``, pages the operator ONCE per outage through
  :class:`~cara.observability.AlertSink` (``critical``, fenced on the last
  tick seen for :data:`ALERT_FENCE_SECONDS`).

The read is one cache GET, memoised per process for :data:`MEMO_SECONDS`.
The heartbeat key outlives any outage (:data:`HEARTBEAT_TTL_SECONDS`) so a
reader can say since when. Best effort on the read side: an unreadable cache
reads NOT delayed, so a cache blip never raises a false alarm.

Product-blind: the product supplies the alert family, the consequence line
for the page and where the verdict is shown.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from cara.facades import Cache, Log
from cara.observability import AlertSink

HEARTBEAT_CACHE_KEY = "scheduler:heartbeat"
#: Long enough to remember the last tick through any outage worth naming.
HEARTBEAT_TTL_SECONDS = 7 * 24 * 3600
#: No tick for this long is an outage (the tick job runs every minute).
STALE_AFTER_SECONDS = 600
#: One page per outage; a scheduler still down after this long pages again.
ALERT_FENCE_SECONDS = 6 * 3600
#: A process re-reads the heartbeat at most this often.
MEMO_SECONDS = 30

_memo: dict[str, Any] = {"at": 0.0, "key": None, "status": None}


def _utc(moment: datetime | None) -> datetime:
    if moment is None:
        return datetime.now(UTC)
    if moment.tzinfo is None:
        raise ValueError("scheduler heartbeat times must be timezone-aware")
    return moment.astimezone(UTC)


def record_scheduler_tick(now: datetime | None = None) -> None:
    """Stamp the heartbeat with ``now`` (default: this instant, UTC)."""
    Cache.put(HEARTBEAT_CACHE_KEY, _utc(now).isoformat(), HEARTBEAT_TTL_SECONDS)


def last_scheduler_tick() -> datetime | None:
    """The last tick written, as an aware UTC datetime, or ``None``."""
    raw = Cache.get(HEARTBEAT_CACHE_KEY)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def reset_scheduler_status_memo() -> None:
    """Forget the per-process memo (tests; a process that must re-read now)."""
    _memo.update(at=0.0, key=None, status=None)


def scheduler_status(
    now: datetime | None = None,
    *,
    alert_family: str | None = None,
    consequence: str = "",
    stale_after_seconds: int = STALE_AFTER_SECONDS,
) -> dict[str, Any]:
    """``{"delayed": bool, "last_tick_at": iso | None}``.

    Delayed when no tick landed within ``stale_after_seconds`` — including
    "never": an environment whose scheduler never ran is exactly the outage
    this exists for. A delayed read pages once per outage when
    ``alert_family`` is given. An explicit ``now`` bypasses the memo.
    """
    clock = time.monotonic()
    memo_key = (alert_family, stale_after_seconds)
    if (
        now is None
        and _memo["status"] is not None
        and _memo["key"] == memo_key
        and clock - _memo["at"] < MEMO_SECONDS
    ):
        return dict(_memo["status"])
    current = _utc(now)
    try:
        last = last_scheduler_tick()
    except Exception as exc:
        Log.warning(
            "scheduler heartbeat unreadable: %s",
            type(exc).__name__,
            category="cara.scheduling",
        )
        return {"delayed": False, "last_tick_at": None}
    delayed = last is None or (current - last).total_seconds() > stale_after_seconds
    status = {
        "delayed": bool(delayed),
        "last_tick_at": last.isoformat() if last is not None else None,
    }
    if delayed and alert_family:
        _page_operator(alert_family, consequence, last, current)
    if now is None:
        _memo.update(at=clock, key=memo_key, status=dict(status))
    return status


def _page_operator(
    family: str, consequence: str, last: datetime | None, now: datetime
) -> None:
    """One critical page per outage, fenced on the last tick seen."""
    seen = last.isoformat() if last is not None else "never"
    fence = f"operator-alert:{family}:{seen}"
    try:
        if not Cache.add(fence, True, ALERT_FENCE_SECONDS):
            return
    except Exception as exc:  # a duplicate page beats a silent one
        Log.warning(
            "scheduler alert fence unavailable: %s",
            type(exc).__name__,
            category="cara.scheduling",
        )
    if last is not None:
        minutes = int((now - last).total_seconds() // 60)
        body = f"No scheduler tick for {minutes} minutes"
    else:
        body = "The scheduler has never written a heartbeat"
    body += "; every recurring job is stalled."
    if consequence:
        body += f" {consequence}"
    body += " Check the schedule:work process."
    try:
        AlertSink.fire(
            severity="critical",
            title="Scheduler stopped ticking",
            body=body,
            dedup_key=f"{family}:heartbeat",
            context={"last_tick_at": last.isoformat() if last else None},
        )
    except Exception as exc:
        Log.warning(
            "scheduler alert not sent: %s",
            type(exc).__name__,
            category="cara.scheduling",
        )
    Log.error("scheduler heartbeat stale: last tick %s", seen, category="cara.scheduling")


__all__ = [
    "ALERT_FENCE_SECONDS",
    "HEARTBEAT_CACHE_KEY",
    "HEARTBEAT_TTL_SECONDS",
    "MEMO_SECONDS",
    "STALE_AFTER_SECONDS",
    "last_scheduler_tick",
    "record_scheduler_tick",
    "reset_scheduler_status_memo",
    "scheduler_status",
]
