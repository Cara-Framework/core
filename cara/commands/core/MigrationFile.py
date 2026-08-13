"""MigrationFile."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MigrationFile:
    """A parsed migration file: classification + the SQL text it contains."""

    path: Path
    generated_table: str | None
    banned_markers: tuple[str, ...]
    docstring: str | None
    sql_constants: tuple[tuple[int, str], ...]
    syntax_error: str | None
