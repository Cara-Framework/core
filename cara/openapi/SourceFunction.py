"""Canonical definition of ``SourceFunction``."""

from __future__ import annotations

import ast
from dataclasses import dataclass

from .SourceClass import SourceClass
from .SourceModule import SourceModule


@dataclass(frozen=True)
class SourceFunction:
    """A function definition, its module, and the class whose body defines
    it (``None`` for a module-level function) (``SourceIndex``)."""

    module: SourceModule
    node: ast.FunctionDef | ast.AsyncFunctionDef
    owner: SourceClass | None = None
