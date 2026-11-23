"""``Atomic`` transaction support for Cara ORM.

Provides @atomic decorator and context manager for clean transaction
handling with automatic savepoint support for nested transactions.

One ``__exit__`` path: the resolver
-----------------------------------
Every level commits and rolls back through the resolver, which nests by
``transaction_level`` exactly as ``DB.transaction()`` does: at the
outermost level it clears the registry entry and returns the psycopg2
connection to the pool; at a nested level it releases (or rolls back to)
the savepoint and leaves the outer transaction pinned.

A nested ``atomic`` used to call ``connection.commit()`` /
``connection.rollback()`` directly (from when the resolver could not
nest). Bypassing the resolver cost three things:

* a ``RELEASE SAVEPOINT`` that failed — PostgreSQL refuses it (25P02) once
  a statement inside the block failed and was swallowed — left the level
  un-decremented, so the outer rollback only rolled back to the savepoint:
  the OUTER transaction stayed open and pinned, with its pool permit, and
  every later statement of that context ran inside a transaction nobody
  would ever commit;
* callbacks registered in a nested block that rolled back still fired on
  the outer commit, and its after-rollback callbacks never ran;
* a COMMIT that failed at the outermost level (a deferred constraint, a
  serialization failure reported at commit) was never unwound: the wrapper
  stayed pinned and kept its permit, where ``DB.transaction()`` rolls back
  and releases (``ConnectionResolver.rollback_failed_commit``).

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

import asyncio
import inspect
import time
from functools import wraps

from cara.eloquent.Integrity import sqlstate_of
from cara.facades import DB

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

    Extraction is delegated to ``cara.eloquent.Integrity.sqlstate_of``, the
    one owner of "get a SQLSTATE out of a possibly-wrapped driver exception".
    This function used to read ``exc.pgcode`` directly, which claimed to
    mirror ``Integrity`` but dropped the cause-chain unwrap that makes
    ``Integrity`` work — and ``PostgresConnection.query`` re-raises every
    Class-40 error as ``DatabaseUnavailableException(str(e)) from e``, an
    ORMException with no ``pgcode``. So the predicate was False on the first
    attempt every time and ``atomic(attempts=N)`` retried ZERO times in
    production, invisibly: nothing was swallowed, the closure simply
    propagated exactly as ``attempts=1`` would.

    ``_RETRIABLE_SQLSTATES`` stays here — the set of codes worth re-running a
    transaction for is this module's own vocabulary, stated once. Only the
    extraction mechanism was restated.
    """
    return sqlstate_of(exc) in _RETRIABLE_SQLSTATES


class Atomic:
    """Transaction context manager and decorator with savepoint support."""

    def __init__(self, connection=None, attempts: int = 1):
        self.connection_name = connection
        # Total times the closure may run on a deadlock / serialization
        # failure. 1 = original no-retry behaviour. Clamped to >= 1.
        self.attempts = max(1, int(attempts))
        self._db = None
        self._conn = None
        # Track whether THIS Atomic opened the outer transaction: only the
        # opener is allowed to retry (it can roll the outer transaction
        # back; a nested savepoint caller cannot).
        self._is_outer_transaction = False
        self._connection_name: str | None = None

    def _get_db(self):
        if self._db is None:
            self._db = DB
        return self._db

    def __enter__(self):
        db = self._get_db()
        resolver = db.get_resolver()
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
        resolver = db.get_resolver()
        name = self._connection_name

        try:
            if exc_type is not None:
                resolver.rollback(name)
                return False  # re-raise
            try:
                resolver.commit(name)
            except BaseException:
                resolver.rollback_failed_commit(name)
                raise
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
            except BaseException as exc:  # re-raised below
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
                delay = min(_RETRY_BACKOFF_SECONDS * attempt, _RETRY_BACKOFF_CAP_SECONDS)
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

    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        instance = Atomic(connection=connection_name)
        try:
            with instance:
                return await func(*args, **kwargs)
        except BaseException as exc:  # re-raised below
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
        def transfer(): ...  # re-run up to 3× on deadlock / serialization failure

    Nested usage (inner becomes savepoint)::

        with atomic():
            Product.create({...})
            with atomic():
                ProductImage.create({...})
    """
    return Atomic(connection=connection, attempts=attempts)
