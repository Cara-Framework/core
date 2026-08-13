"""One store for attribute writes, one precedence for attribute reads.

``Model`` keeps its values in ``__attributes__`` (persisted) and
``__dirty_attributes__`` (pending), and ``Model.__setattr__`` is the single
write door — it applies mutators, casts and date conversion on the way in.

Two things drifted away from that:

* ``HasAttributes.set_attribute`` is NOT shadowed by ``Model``, so its own
  body ran — and it wrote a parallel ``_attributes`` / ``_original`` /
  ``_changes`` trio that ``Model`` never reads. A public ORM setter silently
  discarded the value: the attribute was missing afterwards and ``save()``
  had nothing to persist.
* ``get_raw_attribute`` read ``__attributes__`` alone, so no read API could
  see an unsaved write at all — ``m.foo = 5`` then ``m.get_attribute("foo")``
  answered ``None`` while ``m.foo`` answered 5. ``__getattr__`` had the
  precedence right and restated it three times; those copies now delegate.

Fixing the read then exposed a third defect the first two had been hiding:
``_cast_attribute`` indexed ``__casts__`` unconditionally, which only never
fired because ``get_attribute`` short-circuits on the ``None`` the broken
read always returned.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.testing.FacadeSwap import swap


@pytest.fixture(scope="module", autouse=True)
def _register_memory_connection():
    """Instantiating a model validates its connection; no query is executed."""
    manager = DatabaseManager(
        "app", {"app": {"driver": "sqlite", "database": ":memory:"}}
    )
    with swap("DB", manager):
        yield


class _Row(Model):
    __table__ = "rows"
    __casts__ = {"count": "int"}


def test_set_attribute_is_the_same_write_as_plain_assignment() -> None:
    assigned = _Row()
    assigned.title = "hello"

    via_method = _Row()
    via_method.set_attribute("title", "hello")

    assert via_method.title == assigned.title == "hello"
    assert via_method.get_attribute("title") == "hello"
    # The value must be where ``save()`` looks for it.
    assert via_method.__dict__["__dirty_attributes__"]["title"] == "hello"
    assert via_method.is_dirty("title")


def test_reads_see_a_pending_write() -> None:
    """``m.foo`` and ``m.get_attribute("foo")`` may never disagree."""
    row = _Row()
    row.title = "pending"

    assert row.title == "pending"
    assert row.get_attribute("title") == "pending"
    assert row.get_raw_attribute("title") == "pending"


def test_pending_write_outranks_the_persisted_value() -> None:
    row = _Row()
    row.__attributes__["title"] = "stored"

    assert row.get_raw_attribute("title") == "stored"

    row.title = "edited"

    assert row.get_raw_attribute("title") == "edited"
    assert row.__attributes__["title"] == "stored", "the persisted value is untouched"


def test_declared_casts_still_apply_through_set_attribute() -> None:
    row = _Row()
    row.set_attribute("count", "42")

    value = row.get_attribute("count")

    assert value == 42
    assert isinstance(value, int)


def test_an_undeclared_attribute_reads_back_uncast() -> None:
    """No cast declared is not an error — it is the common case."""
    row = _Row()
    row.set_attribute("nickname", "cara")

    assert row.get_attribute("nickname") == "cara"


def test_a_missing_attribute_reads_as_none() -> None:
    assert _Row().get_raw_attribute("never_set") is None
