"""``NothingDue``: the answer of a notification send that had nothing to do."""

from __future__ import annotations


class NothingDue:
    """Every channel was skipped by ``should_send`` — not a failure.

    A send answered ``False`` both when a channel failed and when nothing
    was due, so a queued send of a deliberately skipped notification raised
    and retried. The singleton :data:`Notification.NOTHING_DUE` stays falsy,
    so a caller that branches on the bool sees exactly what it saw before;
    a caller that needs the difference compares identity
    (``result is Notification.NOTHING_DUE``).
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "NOTHING_DUE"
