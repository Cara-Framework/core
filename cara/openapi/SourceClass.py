"""Canonical definition of ``SourceClass``."""

from __future__ import annotations

import ast
from dataclasses import dataclass

from .SourceModule import SourceModule


@dataclass(frozen=True)
class SourceClass:
    """A class definition and the module it is defined in (``SourceIndex``)."""

    module: SourceModule
    node: ast.ClassDef
