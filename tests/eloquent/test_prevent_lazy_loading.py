"""Tests for the strict lazy-load guard (Laravel's ``Model::preventLazyLoading``).

Contract
~~~~~~~~
* OFF by default — a TOTAL no-op. Every existing query/test keeps lazy-
  loading silently, zero behaviour change.
* ON (``Model.prevent_lazy_loading()``) — accessing an un-eager-loaded
  relation on a COLLECTION-hydrated model (the rows where N+1 actually
  bites) raises ``LazyLoadingViolation`` naming model + relation, BEFORE
  any DB round-trip.
* ON + the relation was eager-loaded (cached in ``_relations``) — no
  raise; the cached value is returned.
* ON + single-instance find()/first() model (NOT collection-hydrated) —
  no raise; lazy-loading one related record off a single find is fine.

The guard fires in each relation descriptor's ``__get__`` *before* the
lazy fetch, so these tests need no live database — when the guard trips,
it raises before any query is built. The OFF/eager paths are asserted by
pre-caching the relation (so no query is issued) or by exercising the
guard helper directly.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.eloquent.relationships import belongs_to, has_many
from cara.exceptions import LazyLoadingViolation


@pytest.fixture(scope="module", autouse=True)
def _register_memory_connection():
    """Register an in-memory sqlite connection so models can instantiate
    (building a model touches the query builder, which validates the
    connection). No query is ever executed in these tests."""
    dm = DatabaseManager.get_instance()
    dm.set_database_config("app", {"app": {"driver": "sqlite", "database": ":memory:"}})
    yield


@pytest.fixture(autouse=True)
def _guard_off_after_each():
    """The guard is process-wide state — always restore OFF so one test
    can never leak the armed flag into another (or the wider suite)."""
    yield
    Model.prevent_lazy_loading(False)


# ── Test models ────────────────────────────────────────────────


class _Comment(Model):
    __table__ = "comment"
    __fillable__ = ["*"]


class _Author(Model):
    __table__ = "author"
    __fillable__ = ["*"]


class _Post(Model):
    __table__ = "post"
    __fillable__ = ["*"]

    @belongs_to("author_id", "id")
    def author(self):
        return _Author

    @has_many("post_id", "id")
    def comments(self):
        return _Comment


def _hydrated_post(from_collection: bool) -> _Post:
    post = _Post()
    post.__attributes__.update({"id": 1, "author_id": 7})
    if from_collection:
        post._mark_from_collection()
    return post


# ── OFF by default = no-op ─────────────────────────────────────


def test_guard_off_by_default_is_noop():
    # Brand-new collection-hydrated model, guard never enabled.
    post = _hydrated_post(from_collection=True)
    # The guard helper must not raise — it's a pure no-op while OFF.
    post._guard_against_lazy_load("author")  # must not raise
    assert Model._prevent_lazy_loading is False


def test_guard_off_belongs_to_access_does_not_raise_violation():
    """With the guard OFF, accessing the relation proceeds past the guard
    into the lazy fetch — it must NOT raise ``LazyLoadingViolation``.
    (It may raise a DB-level error since the table doesn't exist; we only
    assert the violation is NOT what surfaces.)"""
    post = _hydrated_post(from_collection=True)
    try:
        _ = post.author
    except LazyLoadingViolation:
        pytest.fail("guard raised while OFF — must be a no-op by default")
    except Exception:
        # Any non-violation error (e.g. no such table) is fine here — it
        # proves the guard let the access through to the actual fetch.
        pass


# ── ON = collection-hydrated lazy-load raises ──────────────────


def test_on_collection_hydrated_belongs_to_raises():
    Model.prevent_lazy_loading()
    post = _hydrated_post(from_collection=True)
    with pytest.raises(LazyLoadingViolation) as exc:
        _ = post.author
    # Message names both the model and the relation.
    assert "_Post" in str(exc.value)
    assert "author" in str(exc.value)


def test_on_collection_hydrated_has_many_raises():
    Model.prevent_lazy_loading()
    post = _hydrated_post(from_collection=True)
    with pytest.raises(LazyLoadingViolation):
        _ = post.comments


# ── ON + eager-loaded relation = no raise ──────────────────────


def test_on_eager_loaded_relation_does_not_raise():
    """An eager-loaded relation is cached in ``_relations`` (exactly where
    ``with_(...)`` puts it). The guard sees it already loaded and returns
    the cached value without raising."""
    Model.prevent_lazy_loading()
    post = _hydrated_post(from_collection=True)
    sentinel = _Author()
    sentinel.__attributes__.update({"id": 7, "name": "Ada"})
    # Simulate the eager-load having populated the relation cache.
    post.add_relation({"author": sentinel})

    # No raise — and the cached eager value comes back.
    assert post.author is sentinel


# ── ON + single-instance find()/first() = no raise ─────────────


def test_on_single_instance_find_is_not_flagged():
    """A model from a single find()/first() is NOT tagged
    ``_from_collection``; lazy-loading one related record off it is
    allowed even with the guard armed (N+1 only matters across a
    collection)."""
    Model.prevent_lazy_loading()
    post = _hydrated_post(from_collection=False)  # single-find shape
    # The guard helper must early-return (not flagged) — no raise.
    post._guard_against_lazy_load("author")  # must not raise
    try:
        _ = post.author
    except LazyLoadingViolation:
        pytest.fail("single-find model wrongly flagged by the lazy-load guard")
    except Exception:
        # Non-violation DB error is fine — proves the access proceeded.
        pass
