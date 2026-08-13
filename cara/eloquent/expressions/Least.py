from __future__ import annotations

from typing import Any

from cara.exceptions import InvalidArgumentException

from .Operation import Operation


class Least:
    """``LEAST(a, b, ...)`` function expression.

    Mirrors the SQL ``LEAST`` scalar function: returns the smallest of its
    arguments. Arguments may be :class:`~cara.eloquent.expressions.F.F`
    column references, nested expressions, or plain Python literals — the
    grammar quotes identifiers and escapes literals when it walks the tree
    (see ``BaseGrammar.compile_expression``).

    Usable two ways:

    * As a SELECT helper via ``QueryBuilder.select_least``::

          q.select_least("price_high", "ceiling_price", alias="effective_high")
          # SELECT LEAST("price_high", "ceiling_price") AS "effective_high"

    * Inside an ``F``-style update so the new value clamps against the
      current column::

          update({"price_low": Least(F("price_low"), value)})
          # SET "price_low" = LEAST("price_low", <value>)

    Like :class:`F`, it carries the same arithmetic protocol so it can be
    composed inside larger expressions.
    """

    # ``function`` is a class constant below — it must NOT be in __slots__ too
    # (Python raises "'function' in __slots__ conflicts with class variable").
    __slots__ = ("arguments",)

    function = "LEAST"

    def __init__(self, *arguments: Any) -> None:
        if len(arguments) < 1:
            raise InvalidArgumentException(
                f"{self.function}() requires at least one argument."
            )
        self.arguments = arguments

    # ── arithmetic protocol (parity with F / Operation) ──────────────

    def __add__(self, other: Any):

        return Operation(self, "+", other)

    def __radd__(self, other: Any):

        return Operation(other, "+", self)

    def __sub__(self, other: Any):

        return Operation(self, "-", other)

    def __rsub__(self, other: Any):

        return Operation(other, "-", self)

    def __mul__(self, other: Any):

        return Operation(self, "*", other)

    def __rmul__(self, other: Any):

        return Operation(other, "*", self)

    def __truediv__(self, other: Any):

        return Operation(self, "/", other)

    def __rtruediv__(self, other: Any):

        return Operation(other, "/", self)

    def __repr__(self) -> str:
        args = ", ".join(repr(a) for a in self.arguments)
        return f"{self.function}({args})"
