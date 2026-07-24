"""Prunable model concern — reusable batched-pruning primitive.

Laravel parity (``Illuminate\\Database\\Eloquent\\Prunable`` /
``MassPrunable``). A model opts in by mixing :class:`MakesPrunable` and
overriding :meth:`prunable` to return the query identifying the rows that
should be pruned (e.g. "outbound clicks older than 90 days")::

    class OutboundClick(Model, MakesPrunable):
        __table__ = "outbound_click"

        def prunable(self):
            cutoff = pendulum.now("UTC").subtract(days=90).to_datetime_string()
            return self.query().where("created_at", "<", cutoff)


    OutboundClick().prune()  # delete the prunable set in batches
    OutboundClick().prune(batch_size=500)

:meth:`prune` walks the prunable set in batches of ``batch_size`` primary
keys, deleting each batch and returning the total number of rows removed.
Batching keeps a single prune from loading millions of rows into one
statement (and one transaction) — each batch is its own DELETE, so a long
prune doesn't pin a giant lock or balloon the WAL.

Soft-delete interaction
~~~~~~~~~~~~~~~~~~~~~~~~~
The prune respects each model's delete semantics by default:

* A model WITHOUT soft-deletes prunes with an ordinary ``delete()`` (a
  real ``DELETE``).
* A model WITH :class:`MakesSoftDeletes` prunes with a *soft* delete
  (stamping ``deleted_at``) — matching Laravel's ``Prunable``, where
  pruning a soft-deletable model still goes through the model's normal
  delete path.

Set ``__force_prune__ = True`` on the model (or pass ``force=True`` to
:meth:`prune`) to permanently remove rows instead — the
``MassPrunable`` behaviour, where pruning bypasses soft-deletes and issues
a hard ``DELETE``. This is the right choice for high-volume ephemeral
tables (clickstream, raw scrape rows) where keeping tombstones forever
defeats the point of pruning.
"""

from __future__ import annotations

from typing import Any


class MakesPrunable:
    """Mixin giving a model a batched :meth:`prune` over its :meth:`prunable` set.

    Discovered by ``Model.boot`` like every other ``Makes*`` mixin — the
    boot loop calls ``boot_MakesPrunable(builder)`` for any model that
    mixes this in, so the no-op hook below must exist even though pruning
    needs no global scope or macro wiring.
    """

    # Models override to force a hard ``DELETE`` even when soft-deletes are
    # enabled (Laravel ``MassPrunable`` semantics). Default keeps the
    # model's normal delete path (soft for soft-deletable models).
    __force_prune__: bool = False

    def boot_MakesPrunable(self, builder) -> None:
        """No-op boot hook.

        Pruning is a per-call behaviour, not a global scope or query
        macro, so there is nothing to register on the builder. The hook
        exists solely because ``Model.boot`` invokes ``boot_Makes<Name>``
        for every ``Makes*`` base class it finds in the MRO.
        """

    def prunable(self) -> Any:
        """Return the query builder selecting the rows eligible for pruning.

        Must be overridden by the model. The returned builder is expected
        to be a fresh, model-bound query (typically ``self.query().where(...)``)
        — :meth:`prune` re-invokes this for every batch so the set is
        re-evaluated against the current table state after each delete.
        """
        raise NotImplementedError(
            f"{type(self).__name__} mixes in MakesPrunable but does not override "
            "prunable(). Return a query builder selecting the rows to prune, "
            "e.g. self.query().where('created_at', '<', cutoff)."
        )

    def prune(self, batch_size: int = 1000, *, force: bool | None = None) -> int:
        """Delete the prunable set in batches and return the count removed.

        Args:
            batch_size: Maximum rows deleted per batch / per DELETE
                statement. Must be a positive integer.
            force: When ``True``, hard-delete even on a soft-deletable
                model (``MassPrunable`` semantics). When ``False``, always
                use the model's normal delete path. When ``None`` (default),
                fall back to the model's ``__force_prune__`` flag.

        Returns:
            Total number of rows pruned across all batches.

        The loop fetches up to ``batch_size`` primary keys from
        :meth:`prunable`, deletes exactly those rows, and repeats until a
        batch comes back empty. Deleting by explicit primary-key set (rather
        than ``LIMIT`` on the DELETE itself) keeps the count exact and works
        identically whether the delete is soft or hard — a soft delete
        removes the row from the *next* ``prunable()`` evaluation (the
        soft-delete select scope hides it), so the loop still terminates.
        """
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size < 1
        ):
            raise ValueError(
                f"prune() batch_size must be a positive integer, got {batch_size!r}."
            )

        force_delete = self.__force_prune__ if force is None else force
        primary_key = self._prune_primary_key()
        total = 0

        while True:
            ids = list(self.prunable().take(batch_size).pluck(primary_key))
            if not ids:
                break

            total += self._prune_delete(primary_key, ids, force_delete)

            # Last (partial) batch — nothing more can match. Avoids one
            # extra round-trip that would always come back empty.
            if len(ids) < batch_size:
                break

        return total

    # ── internals ──────────────────────────────────────────────────────

    def _prune_primary_key(self) -> str:
        """Resolve the model's primary-key column name for the delete set."""
        getter = getattr(self, "get_primary_key", None)
        if callable(getter):
            return getter()
        return getattr(self, "__primary_key__", "id")

    def _prune_delete(self, primary_key: str, ids: list, force_delete: bool) -> int:
        """Delete one batch of rows by primary key and return the affected count.

        Hard-deletes via the soft-delete ``force_delete_query`` macro when
        ``force_delete`` is set and the model is soft-deletable; otherwise
        runs the model's normal ``delete()`` path (soft for soft-deletable
        models, hard for the rest).
        """
        builder = self.query().where_in(primary_key, ids)

        if force_delete and self._prune_is_soft_deletable():
            # ``force_delete_query`` strips the soft-delete scopes and hands
            # back a builder primed for a real DELETE; execute it ourselves.
            builder = builder.force_delete_query()

        result = builder.delete()
        return self._prune_affected_count(result, ids)

    def _prune_is_soft_deletable(self) -> bool:
        """True when this model mixes in soft-deletes (has a deleted_at column)."""
        return callable(getattr(self, "get_deleted_at_column", None))

    @staticmethod
    def _prune_affected_count(result: Any, ids: list) -> int:
        """Normalise a delete() return value into an affected-row count.

        ``QueryBuilder.delete`` returns the driver's affected-row count, but
        soft-deletes go through an UPDATE and some drivers/doubles return a
        truthy non-int. Fall back to the batch size we asked to delete so the
        running total stays meaningful regardless of driver quirks.
        """
        if isinstance(result, bool):
            return len(ids) if result else 0
        if isinstance(result, int):
            return result
        return len(ids)
