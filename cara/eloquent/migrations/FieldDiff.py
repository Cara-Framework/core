"""Canonical definition of ``FieldDiff``."""

from __future__ import annotations

from dataclasses import dataclass, field

from .MigrationColumn import MigrationColumn


@dataclass
class FieldDiff:
    """One typed schema change. ``kind`` is the discriminator the generator
    branches on."""

    kind: str  # "added" | "removed" | "altered" | "renamed"
    name: str
    column: MigrationColumn | None = None  # added / removed / altered(new) / renamed(new)
    old: MigrationColumn | None = None  # altered(previous) / renamed(previous)
    old_name: str | None = None  # renamed: the previous column name
    changed_attrs: list[str] = field(default_factory=list)  # altered: which attrs

    @property
    def is_destructive(self) -> bool:
        """A removed column drops data; the generator marks/guards these."""
        return self.kind == "removed"

    def __str__(self) -> str:  # human-readable line for the command's diff print
        if self.kind == "added":
            return f"+ add column {self.name} ({self.column.type})"
        if self.kind == "removed":
            return f"- DROP column {self.name} (DESTRUCTIVE)"
        if self.kind == "altered":
            return f"~ alter column {self.name}: {', '.join(self.changed_attrs)}"
        if self.kind == "renamed":
            return f"> rename column {self.old_name} -> {self.name}"
        return f"{self.kind} {self.name}"
