"""``Model.first_or_create`` / ``Model.update_or_create`` — race
condition between SELECT-then-INSERT.

The bug
-------
Pre-fix both methods executed:

    record = cls().where(wheres).first()
    if not record:
        return cls().create(merged_payload)
    return record

Classic TOCTOU. Two requests with the same ``wheres`` arrive
microseconds apart, both pass the ``first()`` SELECT (no row yet),
both call ``create()``. Outcomes:

  * No UNIQUE constraint on ``wheres`` → both INSERTs succeed →
    two rows where the function name promised one. Subsequent
    ``first()`` calls return whichever Postgres picks; later
    update_or_create calls then operate on whichever the SELECT
    happens to return, leaving the other forever desynced.

  * UNIQUE constraint backing ``wheres`` (the load-bearing case
    for real use of these methods — they're called specifically
    BECAUSE the caller expects at-most-one-row semantics) →
    racing INSERT raises ``IntegrityError`` (psycopg2
    ``UniqueViolation`` with SQLSTATE ``23505``). The loser of
    the race surfaces an unhandled 500 even though the row IS
    there now — just inserted by the winner.

Mirror of the unique-violation-translation pattern used elsewhere:
catch the unique-violation SQLSTATE / message and re-query so the
loser returns the winner's row.

Tests pin:
  - Race on first_or_create: ``IntegrityError(23505)`` → re-query
    → return the row the winner inserted.
  - Race on update_or_create: same, then APPLY the update (the
    "upsert" semantics — both racing payloads converge on one row
    with the latest merge).
  - Non-uniqueviolation errors RE-RAISE unchanged (foreign key
    violation, NOT NULL violation, etc.) — we only catch the
    documented race surface.
  - SQLSTATE attribute, ``orig.pgcode`` attribute, and
    ``"duplicate key"`` message form all match the unique-
    violation detector (drivers vary on how they surface it).
  - Re-query returning None after the IntegrityError re-raises
    the original error — vanishing-row race (concurrent delete)
    surfaces the real failure instead of a misleading None.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

_model_mod = importlib.import_module("cara.eloquent.models.Model")
Model = _model_mod.Model


# ── Helpers ────────────────────────────────────────────────────


def _make_unique_violation(*, sqlstate: bool = True) -> Exception:
    """psycopg2 / psycopg3-shaped exception with SQLSTATE 23505."""
    exc = Exception("duplicate key value violates unique constraint")
    if sqlstate:
        exc.sqlstate = "23505"
    return exc


def _make_orig_pgcode_violation() -> Exception:
    """SQLAlchemy-style wrapped: original driver exception under .orig."""

    class _Orig:
        pgcode = "23505"

    exc = Exception("integrity error")
    exc.orig = _Orig()
    return exc


def _make_other_integrity_error() -> Exception:
    """A non-unique IntegrityError (e.g. FK violation). MUST re-raise
    — we only catch the documented race surface for first_or_create."""
    exc = Exception("insert or update on table violates foreign key constraint")
    exc.sqlstate = "23503"  # foreign_key_violation
    return exc


# ── _is_unique_violation detector ─────────────────────────────


class TestIsUniqueViolation:
    """The detector must catch every shape Postgres / SQLAlchemy /
    MySQL surface. Order of precedence: SQLSTATE attribute → wrapped
    ``orig.pgcode`` → message-substring fallback. Each tested
    independently so a driver change that drops one signal still
    leaves the others working."""

    def test_sqlstate_23505_detected(self) -> None:
        assert Model._is_unique_violation(_make_unique_violation()) is True

    def test_orig_pgcode_23505_detected(self) -> None:
        assert Model._is_unique_violation(_make_orig_pgcode_violation()) is True

    def test_duplicate_key_message_detected(self) -> None:
        # No SQLSTATE, no .orig — message-substring fallback.
        exc = Exception(
            'ERROR: duplicate key value violates unique constraint "foo_pkey"'
        )
        assert Model._is_unique_violation(exc) is True

    def test_unique_constraint_message_detected(self) -> None:
        # MySQL flavour.
        exc = Exception(
            "1062 (23000): Duplicate entry for key 'PRIMARY' — unique constraint"
        )
        assert Model._is_unique_violation(exc) is True

    def test_other_sqlstate_not_detected(self) -> None:
        # ``23503`` is foreign_key_violation — must NOT be treated
        # as a unique-violation re-query opportunity. The whole point
        # of the narrow detector is that FK violations re-raise so
        # the caller sees the real bug.
        assert Model._is_unique_violation(_make_other_integrity_error()) is False

    def test_unrelated_exception_not_detected(self) -> None:
        assert Model._is_unique_violation(ValueError("not a db error")) is False


# ── first_or_create race semantics ────────────────────────────


class _RecordingModelFOC:
    """Stand-in for the model class. Records calls so each test can
    assert on the dispatch sequence without a live DB."""

    # Class-level state — reset per test via fixture below.
    select_sequence: list[Any] = []  # ordered .first() return values
    select_calls: list[dict] = []  # captured where() args per first()
    create_side_effect: Exception | None = None
    create_payload: dict | None = None
    primary_key: str = "id"

    # Inherit the unique-violation detector from the real Model —
    # otherwise the classmethod under test calls ``cls._is_unique_
    # violation`` on the stub and AttributeError out.
    _is_unique_violation = staticmethod(Model._is_unique_violation)

    @classmethod
    def get_primary_key(cls) -> str:
        return cls.primary_key

    def __init__(self) -> None:
        # Per-instance .where(wheres).first() captures.
        self._pending_wheres: dict | None = None

    def where(self, wheres: dict) -> _RecordingModelFOC:
        # Chainable — return self, stash wheres so first() can
        # record them.
        self._pending_wheres = dict(wheres)
        return self

    def first(self) -> Any:
        type(self).select_calls.append(dict(self._pending_wheres or {}))
        if not type(self).select_sequence:
            return None
        return type(self).select_sequence.pop(0)

    def create(self, payload: dict, id_key: str | None = None) -> Any:
        type(self).create_payload = dict(payload)
        if type(self).create_side_effect is not None:
            raise type(self).create_side_effect
        # Return a fake "fresh row" so callers can chain.
        return _FakeRow(id=42, **payload)

    def update(self, payload: dict) -> int:
        type(self).create_payload = dict(payload)  # reuse the slot
        return 1


class _FakeRow:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def fresh(self) -> _FakeRow:
        return self


@pytest.fixture
def model_class() -> type[_RecordingModelFOC]:
    """Reset the per-class recording state between tests so each test
    starts from a clean slate."""
    _RecordingModelFOC.select_sequence = []
    _RecordingModelFOC.select_calls = []
    _RecordingModelFOC.create_side_effect = None
    _RecordingModelFOC.create_payload = None
    return _RecordingModelFOC


class TestFirstOrCreateRace:
    def test_existing_row_returned_no_create(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """Baseline happy path — row already exists, no create fires.
        Regression guard against the fix accidentally always going
        through the create branch."""
        existing = _FakeRow(id=1, slug="foo")
        model_class.select_sequence = [existing]

        # Call first_or_create through the real classmethod on the
        # stand-in class. The classmethod is defined on Model and
        # cls() is _RecordingModelFOC().
        out = Model.first_or_create.__func__(model_class, {"slug": "foo"}, {})
        assert out is existing
        assert model_class.create_payload is None  # create never called

    def test_no_row_no_race_creates_normally(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """Happy path when no concurrent inserter exists — first()
        returns None, create() succeeds, that's it."""
        model_class.select_sequence = [None]
        out = Model.first_or_create.__func__(
            model_class,
            {"slug": "foo"},
            {"title": "Foo"},
        )
        assert out.slug == "foo"
        assert out.title == "Foo"
        assert model_class.create_payload == {"title": "Foo", "slug": "foo"}

    def test_race_uniqueviolation_requeries_and_returns_winner_row(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """The load-bearing case. First .first() returns None (race
        opens), create() raises UniqueViolation (loser of the race),
        second .first() returns the winner's row, function returns
        that row instead of bubbling the IntegrityError."""
        winner_row = _FakeRow(id=99, slug="foo", inserted_by="winner")
        model_class.select_sequence = [None, winner_row]
        model_class.create_side_effect = _make_unique_violation()

        out = Model.first_or_create.__func__(model_class, {"slug": "foo"}, {})

        assert out is winner_row
        assert len(model_class.select_calls) == 2, (
            "expected pre-create SELECT + post-violation re-query"
        )

    def test_non_unique_integrity_error_propagates(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """FK violation, NOT NULL violation, check constraint — these
        are NOT the race surface. They must re-raise so the caller
        sees the real bug instead of a misleading retry path that
        silently returns None / wrong row."""
        model_class.select_sequence = [None]
        model_class.create_side_effect = _make_other_integrity_error()

        with pytest.raises(Exception) as excinfo:
            Model.first_or_create.__func__(model_class, {"slug": "foo"}, {})

        # Re-raised the FK error verbatim — no swallow, no re-query.
        assert getattr(excinfo.value, "sqlstate", None) == "23503"
        # Only the original SELECT fired; the re-query did NOT
        # happen (we don't re-query on non-unique violations).
        assert len(model_class.select_calls) == 1

    def test_race_then_winner_vanishes_reraises_original(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """Pathological case — caller A inserts and immediately
        deletes; caller B's pre-INSERT SELECT misses, INSERT loses
        the race against A's INSERT (UniqueViolation), then B's
        re-query also misses (A's row is gone). With no row to
        return, we'd otherwise return None silently — pin that we
        re-raise the original IntegrityError so the caller sees
        the real failure."""
        model_class.select_sequence = [None, None]  # original + re-query both miss
        model_class.create_side_effect = _make_unique_violation()

        with pytest.raises(Exception) as excinfo:
            Model.first_or_create.__func__(model_class, {"slug": "foo"}, {})

        assert getattr(excinfo.value, "sqlstate", None) == "23505"


# ── update_or_create race semantics ───────────────────────────


class TestUpdateOrCreateRace:
    def test_existing_row_updated_no_create(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        existing = _FakeRow(id=1, slug="foo", title="Old")
        # Two .first() calls: pre-update SELECT + post-update SELECT.
        updated = _FakeRow(id=1, slug="foo", title="New")
        model_class.select_sequence = [existing, updated]

        out = Model.update_or_create.__func__(
            model_class,
            {"slug": "foo"},
            {"title": "New"},
        )

        assert out is updated
        # UPDATE was called with the merged payload (updates + wheres).
        assert model_class.create_payload == {"title": "New", "slug": "foo"}

    def test_race_uniqueviolation_falls_through_to_update(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """The upsert promise — loser's payload still lands. SELECT
        misses, CREATE loses the race, the existence-check confirms
        the row IS there, then the UPDATE branch runs and the final
        SELECT returns the row with the loser's merged payload."""
        post_check_row = _FakeRow(id=99, slug="foo", title="Winner")  # existence check
        final_row = _FakeRow(id=99, slug="foo", title="Loser merged")
        # Sequence: pre-create SELECT (miss) → post-violation existence
        # check (hit) → post-update SELECT (final).
        model_class.select_sequence = [None, post_check_row, final_row]
        model_class.create_side_effect = _make_unique_violation()

        out = Model.update_or_create.__func__(
            model_class,
            {"slug": "foo"},
            {"title": "Loser merged"},
        )

        assert out is final_row
        # UPDATE ran with the merged payload — that's the "upsert"
        # promise: even though create lost, the loser's payload
        # converges via the update branch.
        assert model_class.create_payload == {"title": "Loser merged", "slug": "foo"}

    def test_race_then_winner_vanishes_reraises(
        self,
        model_class: type[_RecordingModelFOC],
    ) -> None:
        """Same vanishing-row guard as first_or_create — if the
        post-violation existence check misses, the row was deleted
        between CREATE losing and the existence SELECT. UPDATE on a
        non-existent row would match 0 rows; the final SELECT
        returns None; the caller would see a confusing ``None``
        return from an "upsert" call. Bubble the original error
        instead."""
        model_class.select_sequence = [None, None]  # pre + existence both miss
        model_class.create_side_effect = _make_unique_violation()

        with pytest.raises(Exception) as excinfo:
            Model.update_or_create.__func__(
                model_class,
                {"slug": "foo"},
                {"title": "x"},
            )

        assert getattr(excinfo.value, "sqlstate", None) == "23505"
