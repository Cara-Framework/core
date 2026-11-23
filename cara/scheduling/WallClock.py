"""Put a dictionary schedule entry on the wall clock.

Every dict entry the scheduler registers (``_ScheduleRegistration``) passes
:func:`on_wall_clock` first, so this is the one place a schedule meets the
clock:

- An ``interval`` entry is re-anchored to every scheduler start: the job
  store is in memory, so each restart pushed the next fire a whole interval
  out. Live 2026-10-30 (Synkronus): three restarts in seven minutes moved a
  15-minute sweep from 21:28 to 21:45, and a 24h interval on a scheduler
  restarted daily never runs at all. Every interval the clock can divide (a
  minute count that divides the hour, an hour count that divides the day, one
  day) is registered as the matching wall-clock cron instead (``*/15``
  minutes, ``*/6`` hours), so a restart costs at most the slot it straddled.
  Hour-grained work gets a minute of its own (stable per ``id``) so the
  six-hourly sweeps do not all wake together on the hour. The declared
  ``seconds``/``minutes``/``hours`` stay on the entry: they are the cadence a
  schedule board, the throttle budget and the product guards read. A
  sub-minute tick, or a period the clock cannot divide, stays an interval.
- A ``cron`` entry with a minute and no hour means "hourly at :MM", but the
  dict registration fills a missing hour with 0 — it ran once a day at 00:MM.
  The hour is ``*`` here.

Product-blind and idempotent: an entry already on the clock passes unchanged.
"""

from __future__ import annotations

import zlib
from typing import Any

#: The cadence fields an interval entry may declare, in seconds per unit.
_UNIT_SECONDS: dict[str, int] = {
    "seconds": 1,
    "minutes": 60,
    "hours": 3600,
    "days": 86400,
    # Every unit the registration forwards must be counted here: with
    # ``weeks`` missing, ``{"weeks": 1, "hours": 6}`` read as a six-hour
    # period and was moved onto a ``*/6`` cron — 28 runs a week for one.
    "weeks": 604800,
}


def declared_interval_seconds(entry: dict[str, Any]) -> int | None:
    """The period an entry declares through its interval fields, or ``None``
    when it declares none (a native cron) or an unreadable one."""
    total = 0
    try:
        for field, unit in _UNIT_SECONDS.items():
            total += unit * int(entry.get(field) or 0)
    except TypeError, ValueError:
        return None
    return total if total > 0 else None


def on_wall_clock(entry: Any) -> Any:
    """``entry`` as the scheduler registers it: an interval the clock divides
    becomes its wall-clock cron, and an hour-less cron runs every hour. Any
    other entry is returned as it is."""
    if not isinstance(entry, dict):
        return entry
    trigger = entry.get("trigger", "interval")
    if trigger == "cron":
        if "hour" not in entry and "day_of_week" not in entry and "minute" in entry:
            return {**entry, "hour": "*"}
        return entry
    if trigger != "interval":
        return entry
    slot = _cron_slot(entry)
    if slot is None:
        return entry
    return {**entry, "trigger": "cron", **slot}


def _cron_slot(entry: dict[str, Any]) -> dict[str, str] | None:
    """The cron ``hour``/``minute`` of an interval the wall clock divides."""
    seconds = declared_interval_seconds(entry)
    if seconds is None or seconds % 60:
        return None
    minutes = seconds // 60
    if minutes < 60:
        if 60 % minutes:
            return None
        return {"hour": "*", "minute": "*" if minutes == 1 else f"*/{minutes}"}
    if minutes % 60:
        return None
    hours = minutes // 60
    if hours > 24 or 24 % hours:
        return None
    minute = str(zlib.crc32(str(entry.get("id") or "").encode()) % 60)
    if hours == 24:
        return {"hour": "0", "minute": minute}
    return {"hour": "*" if hours == 1 else f"*/{hours}", "minute": minute}


__all__ = ["declared_interval_seconds", "on_wall_clock"]
