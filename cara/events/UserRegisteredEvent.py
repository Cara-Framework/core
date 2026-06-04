"""Concrete event fired when a new user registers."""

from __future__ import annotations

from typing import Any


class UserRegisteredEvent:
    """Event dispatched after a user registration completes."""

    name = "user.registered"

    def __init__(self, user_id: int, email: str, **extra: Any):
        self.user_id = user_id
        self.email = email
        self.extra = extra
        self._stopped = False

    def payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "email": self.email,
            **self.extra,
        }

    def to_dict(self) -> dict[str, Any]:
        return self.payload()

    def stop_propagation(self) -> None:
        """Mark the event so the dispatcher can skip remaining listeners."""
        self._stopped = True

    @property
    def is_propagation_stopped(self) -> bool:
        return self._stopped
