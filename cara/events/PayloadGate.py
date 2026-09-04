"""The one payload gate both dispatch paths share.

An event opts into payload validation by declaring ``REQUIRED_FIELDS``
— the common case — or by implementing ``validate_payload()`` when the
check is something other than "these attributes must be present".

Before this module the two dispatchers (``Event.dispatch`` in-process,
``rebuild_event`` on the queued-listener path) read ONLY
``validate_payload``, so every event that declared ``REQUIRED_FIELDS``
had to hand-copy the same comprehension into a method. Seven copies in
the cheapa API had already drifted — six compared ``in (None, "")`` and
one compared ``is None``.

"Present" means neither ``None`` nor the empty string: an event field
that arrives as ``""`` carries no more information than a missing one,
and ``""`` is exactly what a serialization round-trip produces for an
unset string.
"""

from __future__ import annotations

from typing import Any

__all__ = ["event_payload_gaps"]


def event_payload_gaps(event: Any) -> list[str]:
    """Return the event's missing/invalid required fields (empty = valid).

    An event that declares neither hook is unvalidated and yields ``[]``.
    A ``validate_payload`` override wins over ``REQUIRED_FIELDS``, and its
    exceptions propagate — each dispatcher owns its own policy for those.
    """
    validator = getattr(event, "validate_payload", None)
    if callable(validator):
        return list(validator() or [])
    required = getattr(event, "REQUIRED_FIELDS", None) or ()
    return [name for name in required if getattr(event, name, None) in (None, "")]
