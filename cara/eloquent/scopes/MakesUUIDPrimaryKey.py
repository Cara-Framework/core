from __future__ import annotations

from .UUIDPrimaryKeyScope import UUIDPrimaryKeyScope


class MakesUUIDPrimaryKey:
    """Global scope class to add UUID as primary key to models."""

    def boot_MakesUUIDPrimaryKey(self, builder):
        builder.set_global_scope(UUIDPrimaryKeyScope())
