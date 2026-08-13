"""LoginLocked."""

from __future__ import annotations

from cara.exceptions import AccountLockedException


class LoginLocked(AccountLockedException):
    """Raised when the requested account is currently locked-out.

    Maps to HTTP 429 (via :class:`AccountLockedException`) — Too-Many-
    Requests rather than 403/401 because the credentials might be correct;
    the lockout is policy, not authorisation. The retry-after window is
    included so well-behaved clients know when to come back.
    """

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__(
            "Too many failed login attempts. Try again in "
            f"{max(1, retry_after_seconds // 60)} minute(s).",
            retry_after_seconds=retry_after_seconds,
        )
