"""Canonical definition of ``DateTimeCast``."""

from __future__ import annotations

from datetime import datetime

import pendulum

from cara.exceptions import NaiveDateTimeException

from .BaseCast import BaseCast


class DateTimeCast(BaseCast):
    """
    Advanced DateTime cast with timezone support and custom formats.

    Format codes (Pendulum compatible):
    - YYYY: 4-digit year
    - MM: 2-digit month
    - DD: 2-digit day
    - HH: 2-digit hour (24h)
    - mm: 2-digit minute
    - ss: 2-digit second
    """

    # Format mapping from common formats to Pendulum
    FORMAT_MAP = {
        "Y-m-d H:i:s": "YYYY-MM-DD HH:mm:ss",
        "Y-m-d": "YYYY-MM-DD",
        "d/m/Y": "DD/MM/YYYY",
        "m/d/Y": "MM/DD/YYYY",
        "H:i:s": "HH:mm:ss",
        "H:i": "HH:mm",
        "c": None,  # ISO 8601 format
        # Additional formats
        "d/m/Y H:i:s": "DD/MM/YYYY HH:mm:ss",  # 26/06/2025 21:53:03
        "H:i:s d.m.Y": "HH:mm:ss DD.MM.YYYY",  # 21:53:03 26.06.2025
    }

    def __init__(self, format_string: str | None = None, timezone: str = "UTC"):
        self.format_string = self._convert_format(format_string)
        #: The zone a READ instant is presented in: the cast's own declaration
        #: (``"datetime:<format>,<zone>"``), UTC unless the model says otherwise
        #: (DOCTRINE §7: every timestamp is TIMESTAMPTZ in UTC).
        self.timezone = timezone

    @staticmethod
    def sql_instant(value: datetime) -> str:
        """The UTC wall string an instant is WRITTEN as — to the microsecond.

        Columns are ``timestamptz(6)``, and the rest of the stack writes them
        at full precision: ``NOW()`` in SQL, and pendulum instances bound
        straight into raw statements (psycopg adapts them losslessly). The
        ORM wrote ``to_datetime_string()``, which DROPS the microseconds, so
        an ORM write landed up to a second BEFORE a raw write of the very
        same instant — and every ordering CHECK across the two broke:
        ``sync_task`` (``confirmed_at >= sent_at``) refused essentially
        every push confirmation, ``catalog_publish_run`` refused its own
        completion. The fraction is appended only when there is one, so an
        instant without microseconds renders exactly as before.

        The string carries an explicit ``+00:00``. Postgres reads an
        offset-less literal into ``timestamptz`` in the SESSION timezone, so
        a bare UTC wall string was only correct while every session happened
        to be UTC: a connection whose ``TimeZone`` came from the server
        default, ``PGTZ`` or a pooler instead of ``server_settings`` stored
        every ORM write shifted by that zone's offset (a "queued 10 minutes
        ago" task read as overdue on a UTC+14 session, 2026-11-15).
        """
        utc = pendulum.instance(value).in_timezone("UTC")
        text = utc.to_datetime_string()
        if utc.microsecond:
            text = f"{text}.{utc.microsecond:06d}"
        return f"{text}+00:00"

    def _convert_format(self, format_str: str | None) -> str | None:
        """Convert PHP-style format to Pendulum format."""
        if not format_str:
            return None
        return self.FORMAT_MAP.get(format_str, format_str)

    def get(self, value):
        """The stored instant, as an aware datetime in the cast's declared zone.

        A ``TIMESTAMPTZ`` value IS an instant: reading it needs no zone from
        anywhere, so the read depends on nothing but the value and the cast's
        own declaration. It used to be shifted into ``app.timezone`` — a
        process setting read on every datetime attribute — so any code that
        touched a model outside a booted application (a kernel projection
        folding inside ``EventStore.append``, a bootless test) raised
        ``InvalidConfigurationSetupException`` on its first ``placed_at``. A
        reader that needs a calendar zone converts explicitly
        (``dt.in_timezone(zone)``); the instant never changes.
        """
        if value is None:
            return None

        try:
            if isinstance(value, str):
                dt = pendulum.parse(value, tz="UTC")  # Database always UTC
            elif isinstance(value, datetime):
                dt = pendulum.instance(value, tz="UTC")  # Database always UTC
            else:
                dt = pendulum.parse(str(value), tz="UTC")  # Database always UTC

            # ``pendulum.DateTime`` IS a ``datetime.datetime`` subclass,
            # so returning it directly satisfies any ``isinstance(x,
            # datetime)`` check downstream and keeps the timezone
            # attached. The legacy ``hasattr(dt, "to_datetime")`` fallback
            # to ``datetime.fromtimestamp(dt.timestamp())`` produced a
            # NAIVE LOCAL-TIME datetime — TypeError when compared against
            # ``pendulum.now("UTC")`` and silent local/UTC drift.
            return dt.in_timezone(self.timezone)
        except ValueError, TypeError, OverflowError:
            return None

    @staticmethod
    def instant(value) -> pendulum.DateTime:
        """The UTC instant a WRITE value names — refused when it names none.

        The one write rule for every date-time the ORM stores (DOCTRINE §7):
        an aware ``datetime`` or a string carrying ``Z`` / ``±HH:MM`` IS an
        instant; a naive ``datetime``, an offset-less string or a bare date
        is a wall clock in a zone it does not state, and is refused with
        ``NaiveDateTimeException``. The cast used to read ``app.timezone``
        to guess that zone, so the same write landed on a different instant
        in each product (Synkronus UTC, Cheapa Istanbul) and failed outright
        in a process that had not booted configuration. A caller that owns a
        wall clock names its zone where it builds the value.

        Unparseable input raises ``ValueError`` (the cast's own fail-closed
        ``None`` is decided by ``set``).
        """
        if isinstance(value, datetime):
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                raise NaiveDateTimeException(value)
            return pendulum.instance(value).in_timezone("UTC")

        parsed = pendulum.parse(str(value), tz=None)
        if not isinstance(parsed, datetime):
            raise ValueError(f"{value!r} is not a date-time")
        if parsed.tzinfo is None:
            raise NaiveDateTimeException(value)
        return parsed.in_timezone("UTC")

    def set(self, value):
        """The UTC wall string a write stores; a naive value is refused.

        ``NaiveDateTimeException`` propagates (a write that cannot say which
        instant it means is a bug in the writer); malformed input is unknown
        and stores ``None`` (``test_temporal_cast_fail_closed``). Reading no
        configuration, the cast behaves the same in every process.
        """
        if value is None:
            return None

        try:
            dt = self.instant(value)
        except ValueError, TypeError, OverflowError:
            return None
        return self.sql_instant(dt)
