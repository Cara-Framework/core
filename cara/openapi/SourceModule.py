"""Canonical definition of ``SourceModule``."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, eq=False)
class SourceModule:
    """One parsed module (``SourceIndex``): its own top-level definitions,
    its import bindings and a generated barrel's lazy exports."""

    name: str
    package: str
    path: Path
    functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef]
    classes: dict[str, ast.ClassDef]
    # local name -> (absolute module, attribute), or (module, None) for ``import m``
    imports: dict[str, tuple[str, str | None]]
    lazy: dict[str, tuple[str, str]]
