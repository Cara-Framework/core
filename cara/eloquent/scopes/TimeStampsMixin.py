from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing_extensions import Self  # noqa: F401

from .TimeStampsScope import TimeStampsScope


class TimeStampsMixin:
    """Global scope class to add soft deleting to models."""

    def boot_TimeStampsMixin(self, builder):
        builder.set_global_scope(TimeStampsScope())

    def activate_timestamps(self, boolean=True) -> Self:
        self.__timestamps__ = boolean
        return self
