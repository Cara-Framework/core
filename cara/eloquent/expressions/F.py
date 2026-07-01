from __future__ import annotations

from typing import Any

from .Operation import Operation


class F:
    """Column-reference expression (Laravel ``DB::raw``-of-a-column parity).

    ``F`` lets writes and filters reference a *column* instead of binding
    every value as a parameter. The grammar compiles an ``F`` by quoting
    its column name as an identifier (``"click_count"`` /
    ``table."col"``) — never as a bound ``%s`` value — so SQL like
    ``click_count = click_count + 1`` can be expressed without a raw
    string.

    Arithmetic against a literal or another expression composes lazily into
    an :class:`Operation` tree that the grammar walks at compile time::

        update({"click_count": F("click_count") + 1})
        # SET "click_count" = "click_count" + 1

        update({"price_low": F("price_low") - F("discount")})
        # SET "price_low" = "price_low" - "discount"

        where(F("price_low"), ">", F("price_high"))
        # WHERE "price_low" > "price_high"

    ``F`` carries no grammar reference; identifier quoting and literal
    escaping are the grammar's responsibility (see
    ``BaseGrammar.compile_expression``), keeping the expression dialect-
    agnostic.
    """

    __slots__ = ("column",)

    def __init__(self, column: str) -> None:
        self.column = column

    # ── arithmetic protocol ──────────────────────────────────────────
    # Each operator returns a new Operation node so chaining
    # (``F("a") + 1 - F("b")``) builds a left-leaning tree without
    # mutating either operand.

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
        return f"F({self.column!r})"
