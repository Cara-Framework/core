from __future__ import annotations

from typing import Any

# Supported binary arithmetic operators, mapped to the SQL symbol the
# grammar emits. Anything outside this set is rejected at construction so a
# malformed expression fails loudly rather than producing broken SQL.
_ARITHMETIC_OPERATORS = {"+": "+", "-": "-", "*": "*", "/": "/"}


class Operation:
    """A binary arithmetic node joining two operands with a SQL operator.

    Operands may be :class:`~cara.eloquent.expressions.F.F` column
    references, nested :class:`Operation` trees, function expressions
    (``Greatest`` / ``Least``), or plain Python literals. The grammar
    renders operands recursively — identifiers get quoted, literals get
    escaped as values — and wraps nested operations in parentheses so
    precedence is explicit (see ``BaseGrammar.compile_expression``).

    ``Operation`` is itself an expression, so it supports the same
    arithmetic protocol — ``(F('a') + 1) * 2`` composes into a left-leaning
    tree without mutating any operand.
    """

    __slots__ = ("left", "operator", "right")

    def __init__(self, left: Any, operator: str, right: Any) -> None:
        if operator not in _ARITHMETIC_OPERATORS:
            from cara.exceptions import InvalidArgumentException

            raise InvalidArgumentException(
                f"Unsupported arithmetic operator {operator!r}. "
                f"Expected one of {', '.join(_ARITHMETIC_OPERATORS)}."
            )
        self.left = left
        self.operator = operator
        self.right = right

    def __add__(self, other: Any) -> Operation:
        return Operation(self, "+", other)

    def __radd__(self, other: Any) -> Operation:
        return Operation(other, "+", self)

    def __sub__(self, other: Any) -> Operation:
        return Operation(self, "-", other)

    def __rsub__(self, other: Any) -> Operation:
        return Operation(other, "-", self)

    def __mul__(self, other: Any) -> Operation:
        return Operation(self, "*", other)

    def __rmul__(self, other: Any) -> Operation:
        return Operation(other, "*", self)

    def __truediv__(self, other: Any) -> Operation:
        return Operation(self, "/", other)

    def __rtruediv__(self, other: Any) -> Operation:
        return Operation(other, "/", self)

    def __repr__(self) -> str:
        return f"Operation({self.left!r} {self.operator} {self.right!r})"
