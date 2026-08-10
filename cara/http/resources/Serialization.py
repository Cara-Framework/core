"""Shared serialization helpers for API resources.

Eliminates duplicated opt_* functions across JsonResource and BaseResource.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from math import isfinite
from typing import Any


def opt_float(value: Any) -> float | None:
    """Coerce to float, preserving None. A non-finite float is unknown.

    ``nan`` and ``±inf`` are not JSON. ``json_dumps`` runs with
    ``allow_nan=False`` — correctly, because the bare ``NaN`` / ``Infinity``
    literals Python would otherwise emit are rejected by ``JSON.parse`` in
    every browser — so letting one through did not ship a wrong number, it
    raised ``ValueError: Out of range float values are not JSON compliant``
    from inside ``JsonResource.to_response`` and took the WHOLE payload down
    as a 500. One unpriceable row, and the endpoint stopped answering.

    It arrives without anyone writing ``float("nan")``: PostgreSQL
    ``numeric`` stores ``'NaN'`` literally, so ``Decimal("NaN")`` comes
    straight off a column, and any margin or ratio computed against a zero
    denominator produces one with no column involved at all.

    ``nan`` is the IEEE spelling of "not a number", which is this file's
    definition of unknown, so ``None`` is the honest answer. ``±inf`` is
    genuinely KNOWN and collapsing it is lossy, but JSON has no spelling for
    infinity and the alternative is not a better number — it is the 500.
    ``opt_int`` has always answered ``None`` for all three (``int(nan)``
    raises ``ValueError`` into its ``except``); two helpers reading the same
    column differently was the defect, not the contract.
    """
    if value is None:
        return None
    try:
        number = float(value)
    except TypeError, ValueError:
        return None
    return number if isfinite(number) else None


def opt_int(value: Any) -> int | None:
    """Coerce to int, preserving None."""
    if value is None:
        return None
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def opt_str(value: Any, default: str | None = None) -> str | None:
    """Coerce to a stripped string, preserving None.

    §7: unknown is NULL, never "". The default used to be ``""``, so a
    nullable column serialized as an empty string and the client could no
    longer tell "we have no tracking number" from "the carrier returned a
    blank one". A caller that genuinely wants a floor now asks for it —
    ``opt_str(value, "")`` — which puts the decision where the meaning is
    known.

    A KNOWN-EMPTY string also collapses to ``default``, and that is a
    deliberate coercion rather than an oversight, so it needs its reason on
    the record — ``opt_list`` was fixed for the mirror-image behaviour in
    this same file and the two must not look inconsistent by accident.

    They differ in what emptiness asserts. An empty LIST is a positive fact
    with a cardinality: "we synced this product and it has zero variants" is
    knowledge, and reporting it as ``null`` claims an ignorance we do not
    have. An empty STRING asserts nothing — there is no such thing as a
    tracking number of length zero, so ``""`` in a text column is how a CSV
    import, a trimmed form field or a legacy default writes "no value". Two
    representations of absence reaching the wire as two different JSON values
    would force every client to test both, and the one that reads only
    ``null`` would render "  " as content.

    Whitespace-only collapses for the same reason and by the same argument:
    ``"   "`` is not a shorter piece of text, it is the absence of text.

    ``default`` chooses WHICH spelling of absence goes out; it does not
    restore the distinction, and the docstring claimed it did until an
    auditor read it against the code. ``opt_str(None, "")`` and
    ``opt_str("", "")`` both return ``""`` — deliberately, because the whole
    argument above is that one field carries one representation of "no
    value". A caller that must tell a stored ``""`` from ``NULL`` is asking a
    question this helper exists to answer and should read the column
    directly.
    """
    if value is None:
        return default
    stripped = str(value).strip()
    return stripped if stripped else default


def opt_datetime(value: Any) -> str | None:
    """Coerce a datetime-like value to an ISO-8601 string, preserving None.

    Always emits an explicit timezone offset for datetime values: a
    naive ``datetime`` (no ``tzinfo``) is interpreted as UTC, which
    matches the codebase convention — the DB stores wall-clock UTC
    and the model layer round-trips through pendulum-in-UTC. Without
    the offset, frontend ``new Date(...)`` parses the string as
    browser-local time and two users in different timezones see
    different absolute moments for the same column.

    ``date`` instances (no time component) are returned as plain
    ``YYYY-MM-DD`` — they intentionally carry no time-of-day, so
    appending an offset would lie about precision.

    Datetime-shaped strings (e.g. ``"2026-05-23 12:30:45"`` from a
    raw ``DB.select`` row) are normalised to ISO 8601 with a UTC
    suffix; Safari historically rejects the space-separated form.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    s = str(value).strip() if value else None
    if not s:
        return None
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            date.fromisoformat(s)
            return s
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(s.replace(" ", "T", 1))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.isoformat()
    except ValueError:
        return s


def opt_bool(value: Any, default: bool | None = None) -> bool | None:
    """Coerce to bool, preserving None.

    §7 again, and the sharper half of it: the old ``default=False`` answered
    ``false`` for "we have never checked". ``opt_bool(row.is_verified)`` on a
    nullable column rendered "not verified" as fact, and §12.4's fail-closed
    capability rule was defeated at the serialization layer instead of in the
    UI — undetectably, because the distinction was destroyed before the
    response was built. A caller that wants a floor passes one.
    """
    if value is None:
        return default
    return bool(value)


def opt_list(value: Any) -> list | None:
    """Return a list, preserving None.

    Keys on identity, not truthiness. ``list(value) if value else None``
    inverted the §7 rule in the same file that states it: a resource whose
    list is legitimately empty ("we synced and there are zero variants") was
    serialized as ``null``, i.e. unknown — so the API claimed ignorance when
    it knew, while ``opt_str``/``opt_bool`` claimed knowledge when they did
    not. A falsy non-None input (``0``, ``False``) also silently became
    ``null`` instead of raising on an obviously wrong argument.

    ``str``/``bytes`` is refused rather than iterated, which is the same
    judgement one step further. Both ARE sequences, so ``list("SKU-1")`` is
    ``["S", "K", "U", "-", "1"]``: a text column routed here by mistake
    reached the wire as a five-element array of characters with nothing
    raised anywhere, and a silently plausible list is the one failure this
    helper can produce that no one notices. ``opt_list(0)`` has always
    raised ``TypeError``; a string only escaped it by being iterable.
    A caller that means one string in a list writes ``[value]``.
    """
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(
            f"opt_list received {type(value).__name__}; a string is a sequence "
            f"of characters, not a list of items. Write [value] if that is meant."
        )
    return list(value)
