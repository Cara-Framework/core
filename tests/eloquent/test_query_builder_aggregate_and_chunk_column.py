"""``QueryBuilder.sum`` empty-result contract + ``chunk_by_id``
column-name validation entry-point.

Two small pins extracted from a deep audit of the QueryBuilder
surface. Neither is a vulnerability today (the runtime path was
either defensive-by-coincidence or guarded one layer too deep),
but both close a future-refactor foot-gun.

1. ``sum()`` returns ``None`` on an empty result set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Postgres' ``SUM`` over zero rows returns ``NULL``; psycopg surfaces
that as Python ``None``. The pre-fix docstring read "or 0 if no
results" — incorrect, since every caller that didn't follow the
canonical ``float(qb.sum("…") or 0)`` pattern would have hit
``TypeError`` on the empty-table path (``None`` doesn't multiply).

The contract for the four ``NULL``-on-empty aggregates (``SUM``,
``AVG``, ``MIN``, ``MAX``) is now: return ``None``, caller coerces.
``COUNT`` remains the exception — it returns ``0`` on empty because
the SQL semantics differ.

This test wraps a tiny in-memory ``_run_aggregate`` double so the
contract can be pinned without needing a live DB connection. The
behavioural assertion is: ``sum()`` on the builder returns whatever
``_run_aggregate`` returned, including ``None``.

2. ``chunk_by_id(column=...)`` validates the column up-front
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The keyset cursor calls ``where(column, ">", last_id)`` then
``order_by(column, "asc")``. ``order_by`` validates the column
against ``_ORDER_BY_COLUMN_RE`` (only ``[A-Za-z_]...`` allowed,
optionally one dotted qualifier). ``where`` accepted any string.

Net effect pre-fix: a caller passing ``chunk_by_id(..., column=
"id; DROP TABLE x")`` never reached the DB (``order_by`` raised
ValueError on the second hop), so it was not exploitable. BUT
the WHERE clause was already queued onto the clone AND the
failure surfaced one layer deeper than the call site — a refactor
that reordered the hops, or extracted the column-validation gate
from ``order_by``, would have removed the safety net silently.

Fix moves the validation to ``chunk_by_id``'s entry point. The
runtime contract stays identical for callers passing valid column
names; malicious / typo'd input fails immediately with a clear
error message naming the rejected value AND the allowed shape.
"""

from __future__ import annotations

import pytest


class _StubBuilder:
    """Minimal builder double — only the bits ``sum`` / ``chunk_by_id``
    reach into. Avoids the connection-init cost of a full QueryBuilder."""

    def __init__(self, aggregate_return: object = None) -> None:
        self._aggregate_return = aggregate_return
        self.aggregate_calls: list[tuple[str, str]] = []

    def _run_aggregate(self, function: str, column: str, dry: bool = False) -> object:
        self.aggregate_calls.append((function, column))
        return self._aggregate_return


# ── sum() empty-result contract ─────────────────────────────────────


class TestSumEmptyResult:
    def test_returns_none_when_aggregate_returns_none(self) -> None:
        # The pre-fix docstring claimed "or 0 if no results" — wrong.
        # Pin the real Postgres-on-empty contract: SUM over zero rows
        # is NULL → psycopg → Python None.
        from cara.eloquent.query.QueryBuilder import QueryBuilder

        builder = _StubBuilder(aggregate_return=None)
        # Use ``unbound`` ``sum`` to bypass real connection init.
        result = QueryBuilder.sum(builder, "amount")  # type: ignore[arg-type]

        assert result is None, (
            "sum() over an empty result set MUST return None (SQL "
            "semantics). The previous docstring promised 0; every "
            "caller that didn't coerce with ``or 0`` would have hit "
            "TypeError on the empty-table path."
        )

    def test_returns_value_when_aggregate_returns_number(self) -> None:
        # Happy path sanity — the empty-result fix doesn't change
        # the non-empty contract.
        from cara.eloquent.query.QueryBuilder import QueryBuilder

        builder = _StubBuilder(aggregate_return=1234)
        result = QueryBuilder.sum(builder, "amount")  # type: ignore[arg-type]
        assert result == 1234

    def test_canonical_caller_pattern_or_zero(self) -> None:
        # Document the canonical pattern the docstring now recommends:
        # ``float(qb.sum("…") or 0)``. Pin so the contract is testable
        # in CI alongside the implementation.
        from cara.eloquent.query.QueryBuilder import QueryBuilder

        builder = _StubBuilder(aggregate_return=None)
        result = QueryBuilder.sum(builder, "amount")  # type: ignore[arg-type]
        coerced = float(result or 0)
        assert coerced == 0.0

    def test_passes_through_to_run_aggregate(self) -> None:
        # Defensive: pin that ``sum`` is a thin wrapper over
        # ``_run_aggregate("SUM", column)`` — a future refactor
        # that inlines the SQL or short-circuits the call should
        # be a deliberate, conscious change.
        from cara.eloquent.query.QueryBuilder import QueryBuilder

        builder = _StubBuilder(aggregate_return=42)
        QueryBuilder.sum(builder, "price")  # type: ignore[arg-type]
        assert builder.aggregate_calls == [("SUM", "price")]


# ── chunk_by_id column validation ───────────────────────────────────


class TestChunkByIdColumnValidation:
    """The keyset cursor's column parameter MUST be the canonical
    column-name shape ``[A-Za-z_][A-Za-z0-9_]*`` (optionally with a
    single dotted qualifier). Pre-fix the validation lived inside
    ``order_by`` two hops down; this test pins the entry-point gate."""

    def setup_method(self) -> None:
        # We never call .get() — validation raises before the cursor
        # starts the first chunk. A bare object suffices as ``self``.
        from cara.eloquent.query.QueryBuilder import QueryBuilder

        self._call = QueryBuilder.chunk_by_id

    @pytest.mark.parametrize(
        "good_column",
        [
            "id",
            "user_id",
            "_internal_id",
            "Id",
            "users.id",
            "snake_case.snake_case_col",
            "x.y",
        ],
    )
    def test_valid_column_names_accepted(self, good_column: str) -> None:
        # Validation should accept the documented shape without
        # raising. The callback is never invoked because the clone
        # would need a real connection — we wrap the cursor body
        # in try/except to ignore the AttributeError that follows
        # validation and assert ONLY that the validator passed.
        try:
            self._call(  # type: ignore[misc]
                object(),
                100,
                lambda _r: None,
                column=good_column,
            )
        except ValueError as e:
            # Must NOT be our validator's rejection.
            if "invalid column name" in str(e):
                pytest.fail(
                    f"Valid column {good_column!r} was rejected: {e}",
                )
            raise
        except Exception:
            # Anything else (AttributeError from clone() etc.) means
            # validation passed; we stopped before exercising the
            # rest of the cursor body. Good enough.
            pass

    @pytest.mark.parametrize(
        "bad_column",
        [
            "id; DROP TABLE users",  # injection attempt
            "id) UNION SELECT password FROM",  # parenthesised
            "id--comment",  # SQL comment
            "id\nORDER BY x",  # newline payload
            "id;",  # trailing semicolon
            "user.id.extra",  # 3 segments — only 2 allowed
            "1id",  # starts with digit
            "",  # empty
            "   ",  # whitespace only
            "id col",  # space mid-string
            "id/*comment*/",  # SQL block comment
            "id`",  # backtick
        ],
    )
    def test_invalid_column_names_rejected_with_clear_error(
        self,
        bad_column: str,
    ) -> None:
        with pytest.raises(ValueError, match="invalid column name"):
            self._call(  # type: ignore[misc]
                object(),
                100,
                lambda _r: None,
                column=bad_column,
            )

    def test_non_string_column_rejected(self) -> None:
        # Defence: type-check too, not just the regex. An int /
        # None / list payload would currently fall through the
        # ``re.fullmatch`` and raise TypeError mid-cursor.
        with pytest.raises(ValueError, match="invalid column name"):
            self._call(  # type: ignore[misc]
                object(),
                100,
                lambda _r: None,
                column=123,  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError, match="invalid column name"):
            self._call(  # type: ignore[misc]
                object(),
                100,
                lambda _r: None,
                column=None,  # type: ignore[arg-type]
            )

    def test_default_column_id_passes(self) -> None:
        # Sanity — the default ``column="id"`` must continue to
        # work after the validation gate. Pin so a future refactor
        # that tightens the regex without updating the default
        # fires here, not in the next migrate:reset run.
        try:
            self._call(object(), 100, lambda _r: None)  # type: ignore[misc]
        except ValueError as e:
            if "invalid column name" in str(e):
                pytest.fail(f"Default column 'id' was rejected: {e}")
        except Exception:
            pass  # Post-validation failure is fine; we only test the gate.
