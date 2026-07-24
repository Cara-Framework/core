"""Time related helpers."""

from __future__ import annotations

import pendulum


def parse_human_time(str_time):
    """
    Take a string like 1 month or 5 minutes and returns a pendulum instance.

    Arguments:
        str_time {string} -- Could be values like 1 second or 3 minutes

    Returns:
        pendulum -- Returns Pendulum instance
    """
    if str_time == "now":
        return pendulum.now("UTC")

    if str_time != "expired":
        number = int(str_time.split(" ")[0])
        length = str_time.split(" ")[1]

        if length in ("second", "seconds"):
            return pendulum.now("UTC").add(seconds=number)
        elif length in ("minute", "minutes"):
            return pendulum.now("UTC").add(minutes=number)
        elif length in ("hour", "hours"):
            return pendulum.now("UTC").add(hours=number)
        elif length in ("day", "days"):
            return pendulum.now("UTC").add(days=number)
        elif length in ("week", "weeks"):
            return pendulum.now("UTC").add(weeks=number)
        elif length in ("month", "months"):
            return pendulum.now("UTC").add(months=number)
        elif length in ("year", "years"):
            return pendulum.now("UTC").add(years=number)

        return None
    else:
        return pendulum.now("UTC").subtract(years=20)


def to_pendulum(dt):
    """Coerce a datetime-like value to a timezone-aware pendulum instance.

    Handles offset-naive datetimes (from DB drivers that strip tzinfo),
    pendulum DateTime instances, and string representations. Returns
    ``None`` if coercion fails — the helper is designed for "best
    effort, no exceptions out" use in pipelines where a missing
    timestamp shouldn't tank the calling job.

    Args:
        dt: A datetime-like value (datetime, pendulum.DateTime, str,
            or anything ``pendulum.parse`` can handle). ``None`` is
            silently passed through.

    Returns:
        A timezone-aware ``pendulum.DateTime`` (UTC for naive inputs)
        or ``None`` when coercion fails or input is None.
    """
    from datetime import datetime as _datetime

    # Lazy import: avoids pulling cara.facades into the module-load
    # path of ``cara.support.Time`` (Time is imported very early during
    # bootstrap; Log isn't always available yet).
    if dt is None:
        return None
    if isinstance(dt, pendulum.DateTime):
        return dt
    try:
        if isinstance(dt, _datetime):
            if dt.tzinfo is None:
                return pendulum.instance(dt, tz="UTC")
            return pendulum.instance(dt)
        return pendulum.parse(str(dt), tz="UTC")
    except Exception as e:
        try:
            from cara.facades import Log

            Log.warning(
                "[Time.to_pendulum] coercion failed for value=%s: %s: %s",
                dt,
                e.__class__.__name__,
                e,
                category="datetime",
            )
        except ImportError, RuntimeError:
            # Log facade not booted yet — silently swallow rather
            # than mask the original coercion failure.
            pass
        return None
