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

Both methods now ask ``cara.eloquent.Integrity.is_unique_violation`` —
the framework's single classifier — instead of a private copy.

Re-pinned 2026-08-08
--------------------
``Model._is_unique_violation`` was a second, drifted copy of that
classifier. It read ``exc.sqlstate`` (the psycopg3 name) and
``exc.orig.pgcode`` (the SQLAlchemy name), neither of which exists on
a psycopg2 error nor on the ``QueryException`` the ORM wraps it in, so
its SQLSTATE branch could never be true in this framework and EVERY
decision fell through to substring-matching English message text — the
exact technique ``Integrity``'s module docstring says it exists to
abolish. A Postgres server with non-English ``lc_messages`` therefore
turned a legitimate insert race into an unhandled 500.

The old fixtures here certified that fiction: they hand-set a
``.sqlstate`` attribute and a SQLAlchemy-shaped ``.orig``, shapes no
cara driver produces. They are replaced with real driver exceptions
wrapped exactly as ``PostgresConnection.query`` wraps them
(``raise QueryException(str(e)) from e``), which is what exercises the
cause-chain walk that makes the classifier work.

Tests pin:
  - Race on first_or_create: wrapped ``UniqueViolation(23505)`` →
    re-query → return the row the winner inserted.
  - Race on update_or_create: same, then APPLY the update (the
    "upsert" semantics — both racing payloads converge on one row
    with the latest merge).
  - Non-uniqueviolation errors RE-RAISE unchanged (foreign key
    violation, NOT NULL violation, etc.) — we only catch the
    documented race surface.
  - A non-English violation message with a correct SQLSTATE is still
    detected. This is the bug the re-pin fixes.
  - SQLite, the other driver both products configure, is recognised
    through its message — the one driver that publishes no SQLSTATE.
  - Re-query returning None after the IntegrityError re-raises
    the original error — vanishing-row race (concurrent delete)
    surfaces the real failure instead of a misleading None.
"""

from __future__ import annotations

import importlib
import sqlite3
from typing import Any

import psycopg2.errors
import pytest

from cara.eloquent.Integrity import is_unique_violation, sqlstate_of
from cara.exceptions import QueryException

_model_mod = importlib.import_module("cara.eloquent.models.Model")
Model = _model_mod.Model


# ── Helpers ────────────────────────────────────────────────────
#
# ``pgcode`` is read-only on a real psycopg2 error instance (the driver
# populates it from the server response), so a test-constructed one carries
# ``None``. Subclassing to pin it as a class attribute is the only way to
# build the shape production actually raises.


class _UniqueViolation(psycopg2.errors.UniqueViolation):
    pgcode = "23505"


class _ForeignKeyViolation(psycopg2.errors.ForeignKeyViolation):
    pgcode = "23503"


def _wrapped(driver_error: Exception) -> Exception:
    """Wrap as ``PostgresConnection.query`` does: ``QueryException from e``.

    The driver exception is therefore NOT the exception a caller catches —
    it hangs off ``__cause__``. Any classifier that inspects only the
    top-level object sees nothing.
    """
    try:
        raise QueryException(str(driver_error)) from driver_error
    except QueryException as wrapper:
        return wrapper


def _make_unique_violation() -> Exception:
    """What a lost insert race really looks like coming out of the ORM."""
    return _wrapped(_UniqueViolation("duplicate key value violates unique constraint"))


def _make_other_integrity_error() -> Exception:
    """A non-unique IntegrityError (e.g. FK violation). MUST re-raise
    — we only catch the documented race surface for first_or_create."""
    return _wrapped(
        _ForeignKeyViolation("insert or update on table violates foreign key constraint")
    )


# ── the shared Integrity classifier ───────────────────────────


class TestIsUniqueViolation:
    """Every shape the drivers cara ships actually produce."""

    def test_wrapped_psycopg2_violation_detected(self) -> None:
        assert is_unique_violation(_make_unique_violation()) is True

    def test_bare_psycopg2_violation_detected(self) -> None:
        # Not every call site goes through the ORM wrapper.
        assert is_unique_violation(_UniqueViolation("duplicate key")) is True

    def test_non_english_message_with_correct_sqlstate_detected(self) -> None:
        """The bug this file was re-pinned for.

        Pre-fix the detector matched the English substrings "duplicate key" /
        "unique constraint". On a server with Spanish ``lc_messages`` neither
        appears, the race went undetected, and the loser of a legitimate
        insert race surfaced an unhandled 500 instead of the winner's row.
        """
        spanish = _UniqueViolation(
            'llave duplicada viola restriccion de unicidad "users_email_key"'
        )
        assert is_unique_violation(_wrapped(spanish)) is True

    def test_sqlite_violation_detected(self) -> None:
        """Both products configure a sqlite connection; it has no SQLSTATE."""
        error = sqlite3.IntegrityError("UNIQUE constraint failed: users.email")
        assert is_unique_violation(_wrapped(error)) is True
        assert is_unique_violation(_wrapped(error), column="email") is True
        assert is_unique_violation(_wrapped(error), column="slug") is False

    def test_sqlite_non_unique_integrity_error_not_detected(self) -> None:
        error = sqlite3.IntegrityError("FOREIGN KEY constraint failed")
        assert is_unique_violation(_wrapped(error)) is False

    def test_other_sqlstate_not_detected(self) -> None:
        # ``23503`` is foreign_key_violation — must NOT be treated
        # as a unique-violation re-query opportunity. The whole point
        # of the narrow detector is that FK violations re-raise so
        # the caller sees the real bug.
        assert is_unique_violation(_make_other_integrity_error()) is False

    def test_message_text_alone_is_not_enough(self) -> None:
        """A plain exception that merely MENTIONS a unique index is not a race.

        Pre-fix this returned True, so an exclusion-constraint or deferred-
        constraint report naming ``users_email_key`` was swallowed as a
        "race", the row was re-queried, and a real integrity fault became a
        wrong row or a confusing re-raise.
        """
        exc = Exception('check constraint refers to unique constraint "users_email_key"')
        assert is_unique_violation(exc) is False

    def test_unrelated_exception_not_detected(self) -> None:
        assert is_unique_violation(ValueError("not a db error")) is False


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
        assert sqlstate_of(excinfo.value) == "23503"
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

        assert sqlstate_of(excinfo.value) == "23505"


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

        assert sqlstate_of(excinfo.value) == "23505"
