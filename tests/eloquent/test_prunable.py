"""``MakesPrunable`` — batched prune + count on an in-memory fake model.

These tests pin the mixin's batching loop and return-count contract WITHOUT
a real database by driving it through a fake model whose ``query()`` returns
a tiny in-memory builder over a Python list. The builder implements only the
surface ``MakesPrunable.prune`` touches: ``where_in`` / ``take`` / ``pluck``
/ ``delete`` (and ``force_delete_query`` for the soft-delete path).
"""

from __future__ import annotations

import pytest

from cara.eloquent.concerns import MakesPrunable


class _FakeBuilder:
    """In-memory query builder over a shared ``rows`` dict (id -> row).

    ``prunable_ids`` is recomputed each time a fresh builder is created so a
    soft delete (which stamps ``deleted_at``) drops a row out of the prunable
    set on the next ``prunable()`` evaluation, exactly like the real
    soft-delete select scope.
    """

    def __init__(self, store, *, prunable_only: bool):
        self._store = store
        # Snapshot the candidate ids for THIS builder.
        if prunable_only:
            self._ids = [
                rid for rid, row in store.rows.items() if store.is_prunable(row)
            ]
        else:
            self._ids = list(store.rows.keys())
        self._limit = None
        self._where_in_ids = None
        self._force = False

    def where_in(self, column, ids):
        assert column == "id"
        self._where_in_ids = list(ids)
        return self

    def take(self, n):
        self._limit = n
        return self

    def limit(self, n):
        return self.take(n)

    def pluck(self, column):
        assert column == "id"
        ids = self._ids
        if self._limit is not None:
            ids = ids[: self._limit]
        return list(ids)

    def count(self):
        return len(self._ids)

    def force_delete_query(self):
        self._force = True
        return self

    def delete(self):
        target = self._where_in_ids if self._where_in_ids is not None else self._ids
        affected = 0
        for rid in list(target):
            row = self._store.rows.get(rid)
            if row is None:
                continue
            if self._store.soft_deletes and not self._force:
                # Soft delete: stamp deleted_at; row stays in the table but
                # drops out of the prunable set.
                if row.get("deleted_at") is None:
                    row["deleted_at"] = "2026-07-01 00:00:00"
                    affected += 1
            else:
                del self._store.rows[rid]
                affected += 1
        return affected


class _BasePrunableModel(MakesPrunable):
    """Fake model: in-memory rows, hard delete, prunes rows flagged expired."""

    soft_deletes = False

    def __init__(self):
        self.rows: dict[int, dict] = {}
        self._next_id = 1

    # — bits MakesPrunable / the builder rely on —
    def get_primary_key(self):
        return "id"

    def is_prunable(self, row) -> bool:
        # "expired" rows are prunable; soft-deleted rows are not (gone already).
        return row.get("expired", False) and row.get("deleted_at") is None

    def query(self):
        return _FakeBuilder(self, prunable_only=False)

    def prunable(self):
        return _FakeBuilder(self, prunable_only=True)

    # — test helpers —
    def seed(self, n_expired: int, n_fresh: int = 0):
        for _ in range(n_expired):
            self.rows[self._next_id] = {"id": self._next_id, "expired": True, "deleted_at": None}
            self._next_id += 1
        for _ in range(n_fresh):
            self.rows[self._next_id] = {"id": self._next_id, "expired": False, "deleted_at": None}
            self._next_id += 1


class _SoftDeletePrunableModel(_BasePrunableModel):
    """Soft-deletable fake model — prune stamps deleted_at by default."""

    soft_deletes = True

    def get_deleted_at_column(self):
        return "deleted_at"


# ── batching + count (hard delete) ──────────────────────────────────────


def test_prune_deletes_all_expired_and_returns_count():
    m = _BasePrunableModel()
    m.seed(n_expired=250, n_fresh=40)

    pruned = m.prune(batch_size=100)

    assert pruned == 250, "every expired row pruned, count returned"
    # Fresh rows untouched.
    assert len(m.rows) == 40
    assert all(not r["expired"] for r in m.rows.values())


def test_prune_runs_in_batches_of_batch_size():
    """A 250-row set with batch_size=100 must take 3 batches (100, 100, 50).
    We instrument delete() calls to count the batches."""
    m = _BasePrunableModel()
    m.seed(n_expired=250)

    batch_sizes: list[int] = []
    original = _FakeBuilder.delete

    def _spy(self):
        if self._where_in_ids is not None:
            batch_sizes.append(len(self._where_in_ids))
        return original(self)

    _FakeBuilder.delete = _spy
    try:
        pruned = m.prune(batch_size=100)
    finally:
        _FakeBuilder.delete = original

    assert pruned == 250
    assert batch_sizes == [100, 100, 50], (
        f"expected 3 batches (100,100,50), got {batch_sizes}"
    )


def test_prune_empty_set_returns_zero():
    m = _BasePrunableModel()
    m.seed(n_expired=0, n_fresh=10)
    assert m.prune(batch_size=100) == 0
    assert len(m.rows) == 10


def test_prune_exact_multiple_terminates():
    """batch_size that exactly divides the set must still terminate (the
    loop stops on the first empty batch, not by partial-batch detection)."""
    m = _BasePrunableModel()
    m.seed(n_expired=200)
    assert m.prune(batch_size=100) == 200
    assert len(m.rows) == 0


def test_prune_single_batch_when_smaller_than_batch_size():
    m = _BasePrunableModel()
    m.seed(n_expired=7)
    assert m.prune(batch_size=100) == 7
    assert len(m.rows) == 0


# ── soft-delete interaction ─────────────────────────────────────────────


def test_soft_deletable_model_prunes_softly_by_default():
    """A soft-deletable model's prune stamps deleted_at (rows remain in the
    table) and the loop still terminates because soft-deleted rows leave the
    prunable set."""
    m = _SoftDeletePrunableModel()
    m.seed(n_expired=120)

    pruned = m.prune(batch_size=50)

    assert pruned == 120
    # Rows still present, but all soft-deleted.
    assert len(m.rows) == 120
    assert all(r["deleted_at"] is not None for r in m.rows.values())


def test_force_prune_hard_deletes_soft_deletable_model():
    """``force=True`` (or ``__force_prune__``) hard-deletes even a
    soft-deletable model — MassPrunable semantics."""
    m = _SoftDeletePrunableModel()
    m.seed(n_expired=80)

    pruned = m.prune(batch_size=30, force=True)

    assert pruned == 80
    assert len(m.rows) == 0, "force prune issues a real DELETE"


def test_force_prune_flag_on_model_is_respected():
    class _ForcePruneModel(_SoftDeletePrunableModel):
        __force_prune__ = True

    m = _ForcePruneModel()
    m.seed(n_expired=10)
    assert m.prune(batch_size=5) == 10
    assert len(m.rows) == 0


# ── validation / base-class contract ────────────────────────────────────


def test_invalid_batch_size_raises():
    m = _BasePrunableModel()
    for bad in (0, -1, "100", 3.5, True):
        with pytest.raises(ValueError):
            m.prune(batch_size=bad)


def test_base_prunable_raises_not_implemented():
    """A model that mixes in MakesPrunable but doesn't override prunable()
    gets a clear NotImplementedError, not a silent no-op."""

    class _Unconfigured(MakesPrunable):
        def get_primary_key(self):
            return "id"

    with pytest.raises(NotImplementedError, match="prunable"):
        _Unconfigured().prune()
