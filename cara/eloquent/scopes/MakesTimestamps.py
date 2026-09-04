from __future__ import annotations

from .TimeStampsScope import TimeStampsScope


class MakesTimestamps:
    """Global scope that auto-manages created_at / updated_at columns."""

    def boot_MakesTimestamps(self, builder):
        builder.set_global_scope(TimeStampsScope())
