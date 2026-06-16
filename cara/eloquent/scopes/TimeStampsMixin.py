from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

from .TimeStampsScope import TimeStampsScope


class TimestampsMixin:
    """Global scope that auto-manages created_at / updated_at columns."""

    def boot_TimestampsMixin(self, builder):
        builder.set_global_scope(TimeStampsScope())

    def activate_timestamps(self, boolean=True) -> Self:
        self.__timestamps__ = boolean
        return self
