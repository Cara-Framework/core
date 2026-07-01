"""Atomic transaction support for Cara ORM.

Provides @atomic decorator and context manager for clean transaction
handling with automatic savepoint support for nested transactions.

Two distinct ``__exit__`` paths
-------------------------------
1. **Top-level** (we opened the outer transaction): commit / rollback
   must go through the resolver so the registry entry is cleared and
   the psycopg2 connection is returned to the pool. Calling
   ``self._conn.commit()`` directly bypasses the resolver — the
   connection stays pinned in the per-context registry forever, every
   subsequent query inside that context picks up the same committed
   handle thinking it's still in a transaction, and the pool slowly
   bleeds connections.

2. **Nested savepoint** (the outer transaction already existed when
   ``__enter__`` ran): we own a savepoint, not the connection. The
   underlying ``connection.commit()`` releases the savepoint via the
   driver's transaction-level math and the outer transaction stays
   active. Resolver-level commit must NOT run here — it would unwind
   the outer caller's transaction.

Deadlock / serialization retry
-------------------------------
Postgres aborts one party of a deadlock (SQLSTATE ``40P01``,
``deadlock_detected``) and aborts ``SERIALIZABLE`` / ``REPEATABLE READ``
transactions that lose a write/write race (SQLSTATE ``40001``,
``serialization_failure``). Both are *transient* — the canonical fix is
to roll back and re-run the whole transaction. ``atomic(attempts=N)``
does exactly that: on either code it rolls the transaction back, waits a
brief escalating backoff, and re-invokes the closure, up to ``attempts``
times total.

Retry is ONLY safe when this call **owns the outer transaction**. A
nested ``atomic`` is a savepoint inside someone else's transaction — it
cannot roll the outer transaction back, so re-running its closure against
an already-aborted outer transaction would just fail again (Postgres puts
the whole transaction in the aborted state on a serialization/deadlock
error, not just the savepoint). The savepoint-depth tracking already on
``Atomic`` (``_is_outer_transaction``) is what distinguishes the two: a
nested caller never retries and re-raises immediately so the OUTERMOST
``atomic`` — the one that owns the transaction — is the one that retries.
``attempts=1`` (the default) preserves the original behaviour byte for
byte: no retry, no backoff, single attempt.
"""

from __future__ import annotations

import time
from functools import wraps

# Postgres SQLSTATE codes that are safe to retry by re-running the whole
# transaction. ``40P01`` = deadlock_detected, ``40001`` =
# serialization_failure. Both are Class 40 (Transaction Rollback) and are
# explicitly documented as "retry the transaction".
_RETRIABLE_SQLSTATES = frozenset({"40P01", "40001"})

# Base backoff between retries, in seconds. Escalates linearly with the
# attempt number (attempt 1 → 1×, attempt 2 → 2×, …) with a small fixed
# cap so a contended row doesn't thrash. Kept tiny: these are in-process
# transaction retries, not network backoff.
_RETRY_BACKOFF_SECONDS = 0.05
_RETRY_BACKOFF_CAP_SECONDS = 0.5


def _is_retriable_error(exc: BaseException) -> bool:
    """Return True for a Postgres deadlock / serialization failure.

    Matches on the driver's structured ``pgcode`` (SQLSTATE) only — never
    on message text — mirroring ``cara.eloquent.Integrity.is_unique_violation``.
    Any exception without a retriable ``pgcode`` (including non-DB errors)
    returns False so it propagates immediately.
    """
    return getattr(exc, "pgcode", None) in _RETRIABLE_SQLSTATES


class Atomic:
    """Transaction context manager and decorator with savepoint support."""

    def __init__(self, connection=None, attempts: int = 1):
        self.connection_name = connection
        # Total times the closure may run on a deadlock / serialization
        # failure. 1 = original no-retry behaviour. Clamped to >= 1.
        self.attempts = max(1, int(attempts))
        self._db = None
        self._conn = None
        # Track whether THIS Atomic opened the outer transaction.
        # Only the opener invokes resolver-level commit/rollback — and
        # only the opener is allowed to retry (it can roll the outer
        # transaction back; a nested savepoint caller cannot).
        self._is_outer_transaction = False
        self._connection_name: str | None = None

    def _get_db(self):
        if self._db is None:
            from cara.eloquent.DatabaseManager import DatabaseManager

            self._db = DatabaseManager.get_instance()
        return self._db

    def __enter__(self):
        db = self._get_db()
        resolver = db._ensure_resolver()
        conn_name = self.connection_name or db.get_default_connection()

        # Try to attach to an existing active connection (savepoint
        # path). If that raises, we're the outer caller and must open
        # a real transaction via the resolver — which registers the
        # connection in the per-context registry.
        try:
            self._conn = resolver._get_active_connection(conn_name)
            # Already in a transaction — push a savepoint via the
            # connection's nested-transaction machinery.
            self._conn.begin()
            self._is_outer_transaction = False
        except Exception:
            self._conn = resolver.begin_transaction(conn_name)
            self._is_outer_transaction = True

        self._connection_name = conn_name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always re-resolve the resolver so we don't keep a stale
        # reference if the manager swapped instances mid-flight (rare,
        # but happens during test re-init).
        db = self._get_db()
        resolver = db._ensure_resolver()
        name = self._connection_name

        try:
            if exc_type is not None:
                if self._is_outer_transaction:
                    resolver.rollback(name)
                else:
                    self._conn.rollback()
                return False  # re-raise
            else:
                if self._is_outer_transaction:
                    resolver.commit(name)
                else:
                    self._conn.commit()
                return False
        finally:
            self._conn = None

    def run(self, func, *args, **kwargs):
        """Invoke ``func`` inside a fresh transaction, retrying transient aborts.

        This is the retry-aware execution path used by the decorator. The
        ``with atomic(): ...`` context-manager form intentionally does NOT
        retry — it has no handle on the closure to re-run, only the body of
        the ``with`` block, which it cannot re-enter. Use ``atomic(...)`` as a
        decorator (or call ``run``) when you want retry.

        Retry rules:

        * Each attempt materialises a FRESH :class:`Atomic` so transaction
          state (``_conn`` / ``_is_outer_transaction``) never leaks between
          attempts. The fresh instance's ``__enter__`` re-detects ownership:
          if a transaction is already open in this context, the attempt
          becomes a savepoint and ``_is_outer_transaction`` is False.
        * A retriable error (``40P01`` / ``40001``) is retried ONLY when the
          attempt OWNED the outer transaction. A nested/savepoint attempt
          re-raises immediately so the outermost owner is the one that
          retries — re-running a savepoint closure against an already-aborted
          outer transaction can't succeed.
        * Non-retriable errors, and the final attempt, propagate unchanged.
        """
        connection_name = self.connection_name
        attempts = self.attempts
        last_exc: BaseException | None = None

        for attempt in range(1, attempts + 1):
            instance = Atomic(connection=connection_name)
            try:
                with instance:
                    return func(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001 — re-raised below
                last_exc = exc
                retriable = (
                    _is_retriable_error(exc)
                    # Only the owner of the outer transaction may retry.
                    and instance._is_outer_transaction
                    # Still attempts left.
                    and attempt < attempts
                )
                if not retriable:
                    raise
                # Brief escalating backoff before re-running the closure.
                delay = min(
                    _RETRY_BACKOFF_SECONDS * attempt, _RETRY_BACKOFF_CAP_SECONDS
                )
                if delay > 0:
                    time.sleep(delay)

        # Unreachable in practice (the loop either returns or raises), but
        # keep the contract explicit for type-checkers / defensive callers.
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("atomic.run exhausted its attempts without a result")

    def __call__(self, func):
        """Use as decorator: @atomic() or @atomic(connection='other')

        Each call materializes a *fresh* ``Atomic`` instance — the
        decorator builds at import time, but ``self._conn`` /
        ``self._db`` are per-transaction state. Reusing the import-time
        instance lets two concurrent callers stomp on each other's
        ``_conn``: caller B's commit unwinds caller A's transaction,
        caller A's rollback closes B's connection. Any sync-ORM call
        site decorated with ``@atomic()`` and reached from threads or
        ``asyncio.to_thread`` was vulnerable.

        When ``attempts > 1`` the wrapper runs through :meth:`run`, which
        re-invokes the closure on a deadlock / serialization failure (and
        only when this call owns the outer transaction). At ``attempts == 1``
        the behaviour is identical to the original single-attempt wrapper.

        Supports both sync and async functions: async functions are
        wrapped with an async wrapper so the transaction boundaries
        correctly encompass the awaited coroutine body.
        """
        import inspect

        connection_name = self.connection_name
        attempts = self.attempts

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await _run_async_atomic(
                    func, connection_name, attempts, args, kwargs
                )

            return async_wrapper

        @wraps(func)
        def wrapper(*args, **kwargs):
            return Atomic(connection=connection_name, attempts=attempts).run(
                func, *args, **kwargs
            )

        return wrapper


async def _run_async_atomic(func, connection_name, attempts, args, kwargs):
    """Async sibling of :meth:`Atomic.run` — awaits the coroutine body.

    Mirrors the sync retry contract: a fresh ``Atomic`` per attempt, retry
    only on a retriable SQLSTATE when this attempt owned the outer
    transaction, with a brief escalating backoff between attempts.
    """
    import asyncio

    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        instance = Atomic(connection=connection_name)
        try:
            with instance:
                return await func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 — re-raised below
            last_exc = exc
            retriable = (
                _is_retriable_error(exc)
                and instance._is_outer_transaction
                and attempt < attempts
            )
            if not retriable:
                raise
            delay = min(_RETRY_BACKOFF_SECONDS * attempt, _RETRY_BACKOFF_CAP_SECONDS)
            if delay > 0:
                await asyncio.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("atomic.run exhausted its attempts without a result")


def atomic(connection=None, attempts: int = 1):
    """Create an atomic transaction context manager/decorator.

    Args:
        connection: Optional connection name; defaults to the default
            connection.
        attempts: How many times the closure may run on a deadlock
            (``40P01``) or serialization failure (``40001``). ``1`` (the
            default) preserves the original no-retry behaviour. Retry only
            applies to the DECORATOR / :meth:`Atomic.run` form and only when
            the call owns the outer transaction (a nested savepoint never
            retries).

    Usage as context manager (no retry — see :meth:`Atomic.run`)::

        with atomic():
            Product.create({...})

    Usage as decorator::

        @atomic()
        def my_func():
            Product.create({...})

    Usage as a retrying decorator::

        @atomic(attempts=3)
        def transfer():
            ...  # re-run up to 3× on deadlock / serialization failure

    Nested usage (inner becomes savepoint)::

        with atomic():
            Product.create({...})
            with atomic():
                ProductImage.create({...})
    """
    return Atomic(connection=connection, attempts=attempts)
