"""``QueryBuilder.reset()`` must leave NOTHING of the previous query.

``reset()`` is what makes a builder reusable, and it is called from
inside the compile path (``to_qmark``) — so an incomplete reset does
not fail, it leaks. The leak is invisible in the SQL the caller wrote:
every WHERE and ORDER BY is gone, so the query looks fresh, while a
stale ``LIMIT 1`` left behind by an earlier ``first()`` silently caps
the next result set at one row. A ``lock_for_update().first()``
leaking its lock makes every later query on that builder compile
``FOR UPDATE``; a leaked ``union()`` re-appends the previous UNION;
leaked ``_creates`` poison the next SELECT's column list.

Both historical passes are pinned here — the 2026-06 pass that added
``_limit`` / ``_offset`` / ``_distinct`` / ``_columns`` and the 2026-07
pass that added ``_creates`` / ``_unions`` / ``lock`` /
``_lock_modifier`` — because a "let's tidy this method" refactor is
exactly the change that drops one of them again.
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.eloquent.query import QueryBuilder

# The clean-slate value every field must hold after ``reset()``.
_CLEAN_SLATE: dict[str, Any] = {
    "_updates": (),
    "_wheres": (),
    "_order_by": (),
    "_group_by": (),
    "_joins": (),
    "_having": (),
    "_aggregates": (),
    "_limit": False,
    "_offset": False,
    "_distinct": False,
    "_columns": (),
    "_creates": {},
    "_unions": [],
    "lock": False,
    "_lock_modifier": {"skip_locked": False, "nowait": False, "of": []},
}

# A non-default value for each field, used to dirty the builder first.
_DIRTY: dict[str, Any] = {
    "_updates": ("u",),
    "_wheres": ("w",),
    "_order_by": ("o",),
    "_group_by": ("g",),
    "_joins": ("j",),
    "_having": ("h",),
    "_aggregates": ("a",),
    "_limit": 42,
    "_offset": 100,
    "_distinct": True,
    "_columns": ("x", "y"),
    "_creates": {"name": "leaked"},
    "_unions": [("other-builder", False)],
    "lock": True,
    "_lock_modifier": {"skip_locked": True, "nowait": True, "of": ["product"]},
}


def _dirty_builder() -> QueryBuilder:
    """A connection-free builder with every resettable field dirtied.

    ``__new__`` skips ``__init__`` deliberately: constructing a real
    builder resolves a connection through the ``DB`` facade, and the
    subject here is pure in-memory state.
    """
    builder = QueryBuilder.__new__(QueryBuilder)
    builder._action = "update"
    for name, value in _DIRTY.items():
        setattr(builder, name, value)
    return builder


def test_the_probe_covers_every_documented_field() -> None:
    """The dirty map and the clean-slate map must stay in step.

    Guards the test itself: adding a field to one map and forgetting
    the other would silently stop checking it.
    """
    assert set(_DIRTY) == set(_CLEAN_SLATE)


@pytest.mark.parametrize("field", sorted(_CLEAN_SLATE))
def test_reset_clears_each_field(field: str) -> None:
    """Every field individually, so a failure names the leak."""
    builder = _dirty_builder()

    builder.reset()

    assert getattr(builder, field) == _CLEAN_SLATE[field]


def test_reset_is_a_complete_clean_slate() -> None:
    """All fields at once — a partial reset is the dangerous shape.

    A builder that clears its WHEREs but keeps its LIMIT looks fresh at
    every call site while returning the wrong number of rows.
    """
    builder = _dirty_builder()

    builder.reset()

    assert {name: getattr(builder, name) for name in _CLEAN_SLATE} == _CLEAN_SLATE


def test_reset_returns_to_the_select_action() -> None:
    """An unreset ``update`` action would turn the next read into a write."""
    builder = _dirty_builder()

    builder.reset()

    assert builder._action == "select"


def test_reset_is_fluent() -> None:
    """``reset()`` returns the builder so it can chain."""
    builder = _dirty_builder()

    assert builder.reset() is builder


def test_the_lock_modifier_is_a_fresh_mapping() -> None:
    """Not the same dict every time.

    If ``reset()`` handed back a shared module-level default, one
    builder mutating its lock modifier would rewrite it for every other
    builder in the process.
    """
    first = _dirty_builder().reset()._lock_modifier
    second = _dirty_builder().reset()._lock_modifier

    assert first == second
    assert first is not second

    first["of"].append("product")
    assert second["of"] == []
