"""Canonical query-expression classification and identifier validation."""

from __future__ import annotations

import re

from cara.eloquent.expressions import F, Greatest, Least, Operation

ORDER_BY_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _is_column_expression(value) -> bool:
    """Whether a value renders as a quoted column expression, never a bind."""
    return isinstance(value, (F, Operation, Greatest, Least))
