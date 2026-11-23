from __future__ import annotations

import contextlib
import re
import time
from functools import partial
from typing import Self

from cara.exceptions import (
    DatabaseUnavailableException,
    DriverNotFoundException,
    InvalidArgumentException,
    QueryException,
)

from ..query.grammars import PostgresGrammar
from ..query.processors import PostgresPostProcessor
from ..schema.platforms import PostgresPlatform
from .BaseConnection import BaseConnection
from .PostgresPool import PostgresPool
from .PostgresScopedReads import PostgresScopedReads, scope_snapshot

_SAVEPOINT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

#: How long (seconds) a pooled connection may sit idle and still be handed out
#: WITHOUT the ``SELECT 1`` liveness probe. Outside a transaction every
#: statement checks a connection out and back in, so probing every checkout
#: made each such statement TWO round trips. A connection that answered a
#: statement this recently is alive for every purpose the probe serves; what
#: the probe exists to catch (idle timeouts, a pooler or NAT dropping a quiet
#: socket, a restart during a lull) happens to connections idle for longer,
#: and those are still probed. Per connection:
#: ``connection_pooling_probe_idle_seconds`` (``0`` probes every checkout).
#: The check-in times themselves are each pool's own (``PostgresPool``).
_POOL_PROBE_IDLE_SECONDS = 1.0


class PostgresConnection(PostgresScopedReads, BaseConnection):
    """Postgres Connection class."""

    name = "postgres"

    def __init__(
        self,
        host=None,
        database=None,
        user=None,
        port=None,
        password=None,
        prefix=None,
        options=None,
        full_details=None,
        name=None,
    ):
        self.host = host
        if port:
            self.port = int(port)
        else:
            self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.prefix = prefix
        self.full_details = full_details or {}
        pooling_enabled = self.full_details.get("connection_pooling_enabled", False)
        if not isinstance(pooling_enabled, bool):
            raise InvalidArgumentException(
                "connection_pooling_enabled must be a boolean."
            )
        configured_max = self.full_details.get("connection_pooling_max_size")
        configured_min = self.full_details.get("connection_pooling_min_size", 0)
        if pooling_enabled and (
            isinstance(configured_max, bool)
            or not isinstance(configured_max, int)
            or configured_max <= 0
        ):
            raise InvalidArgumentException(
                "Enabled PostgreSQL pooling requires a positive integer "
                "connection_pooling_max_size."
            )
        if (
            isinstance(configured_min, bool)
            or not isinstance(configured_min, int)
            or configured_min < 0
            or (configured_max is not None and configured_min > configured_max)
        ):
            raise InvalidArgumentException(
                "connection_pooling_min_size must be an integer between zero and "
                "connection_pooling_max_size."
            )
        self.connection_pool_size = configured_max
        self.options = options or {}
        self._cursor = None
        self.transaction_level = 0
        self.open = 0
        self.schema = None
        # Inside ``read_snapshot()``: the block whose session this wrapper
        # leases (never pooled by it; handed back in ``close_connection``),
        # and whether its own transaction was opened in that snapshot.
        self._read_session = None
        self._snapshot_transaction = False
        # The pool whose permit this wrapper holds, or None. A permit goes
        # back to the pool that issued it, whatever the wrapper is asked to
        # connect to afterwards (``set_schema`` changes the pool it resolves).
        self._permit_pool: PostgresPool | None = None
        if name:
            self.name = name

    def make_connection(self):
        """This sets the connection on the connection class."""
        if self.has_global_connection():
            return self.get_global_connection()

        self._connection = self.create_connection()
        if self._read_session is not None:
            # The block's session, set up when it was opened.
            self.open = 1
            return self

        # Post-acquire setup. Pre-fix any failure here (an
        # ``enable_disable_foreign_keys`` SQL roundtrip that hits a
        # dead socket, or the rare ``autocommit = True`` assignment
        # on a TCP-RST'd connection) bubbled straight out of
        # ``make_connection`` — but ``create_connection`` had already
        # acquired a pool slot and assigned ``self._connection``.
        # The caller saw the exception, abandoned the wrapper, and
        # the slot + raw psycopg2 connection stayed orphaned until
        # process exit. Under sustained instability (network flap,
        # Postgres restart that drops in-flight connections) every
        # fire drained 1 slot from the global semaphore; once
        # exhausted, every subsequent caller hung for the full
        # ``_POOL_ACQUIRE_TIMEOUT`` (30 s) and then 503'd. Mirror
        # the ``create_connection`` cleanup contract: on failure,
        # route through ``close_connection`` (releases slot,
        # returns/closes the connection) before re-raising.
        try:
            self._connection.autocommit = True
            self.enable_disable_foreign_keys()
        except BaseException:
            # ``BaseException``: an interrupt between the checkout and the
            # caller's own ``finally`` kept the permit just the same.
            with contextlib.suppress(Exception):
                self.close_connection()
            raise

        self.open = 1

        return self

    _MAX_CONNECT_RETRIES = 5
    _RETRY_BACKOFF_BASE = 0.5

    def _connect_kwargs(self):
        kw = {
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "sslmode": self.options.get("sslmode"),
            "sslcert": self.options.get("sslcert"),
            "sslkey": self.options.get("sslkey"),
            "sslrootcert": self.options.get("sslrootcert"),
        }

        # ``application_name`` and ``connect_timeout`` are first-class
        # psycopg2.connect kwargs — pull them from the config-level
        # ``options`` dict so the values declared in config/database.py
        # actually reach the live connection. Previously these were
        # silently dropped: pg_stat_activity showed empty
        # application_name and connect_timeout fell back to the OS
        # default (~75-130s on Linux), defeating the declared 10s cap.
        app_name = self.options.get("application_name")
        if app_name:
            kw["application_name"] = app_name

        connect_timeout = self.options.get("connect_timeout")
        if connect_timeout is not None:
            with contextlib.suppress(TypeError, ValueError):
                kw["connect_timeout"] = int(connect_timeout)

        # ``server_settings`` is the asyncpg-style nested dict for GUCs
        # we want SET on every fresh session (statement_timeout,
        # lock_timeout, idle_in_transaction_session_timeout, jit,
        # timezone). psycopg2 has no equivalent kwarg — they have to
        # ride in via the connection-level "options" string as
        # ``-c key=value`` flags. Merge with any pre-existing
        # ``search_path`` opt below so neither side wins silently.
        opts_parts = []
        schema = self.schema or self.full_details.get("schema")
        if schema:
            opts_parts.append(f"-c search_path={schema}")

        server_settings = self.options.get("server_settings") or {}
        if isinstance(server_settings, dict):
            for k, v in server_settings.items():
                if v is None:
                    continue
                # GUC names are ASCII identifiers; values may contain
                # spaces (rare for our use). psycopg2 splits on
                # whitespace, so wrap value in single quotes when it
                # contains spaces — but for the GUCs we care about
                # (timeouts, on/off flags, IANA tz strings) this is a
                # no-op and the bare form is what postgres docs show.
                v_str = str(v)
                if " " in v_str:
                    v_str = f"'{v_str}'"
                opts_parts.append(f"-c {k}={v_str}")

        if opts_parts:
            kw["options"] = " ".join(opts_parts)

        return {k: v for k, v in kw.items() if v is not None}

    def _pool(self, dsn: dict) -> PostgresPool:
        """This wrapper's pool: one per configured connection AND per
        resolved DSN/session options (``dsn``: its ``_connect_kwargs()``).

        The name alone is not enough — ``set_schema`` gives one name several
        ``search_path``s, and a session opened under one must not serve the
        other. The DSN alone is not enough either: two names on one database
        are two budgets (``connection_pooling_max_size`` each).
        """
        key = (
            self.connection_name,
            tuple(sorted((option, str(value)) for option, value in dsn.items())),
        )
        label = self.connection_name or (
            f"{dsn.get('host', '')}:{dsn.get('port', '')}/{dsn.get('database', '')}"
        )
        return PostgresPool.of(key, label, self.connection_pool_size)

    _POOL_ACQUIRE_TIMEOUT = 30

    def _release_pool_slot(self) -> None:
        """Hand this wrapper's permit back to the pool that issued it, once."""
        pool, self._permit_pool = self._permit_pool, None
        if pool is not None:
            pool.release()

    def create_connection(self):
        """Inside ``read_snapshot()`` a session of the block's snapshot,
        leased until ``close_connection`` (``PostgresScopedReads``);
        otherwise a pooled connection."""
        leased = self._lease_read_session()
        return leased if leased is not None else self.pooled_connection()

    def pooled_connection(self):
        try:
            import psycopg2  # local: heavy optional dep
        except ModuleNotFoundError as exc:
            raise DriverNotFoundException(
                "The PostgreSQL driver is not installed; install psycopg2-binary."
            ) from exc

        if not self.full_details.get("connection_pooling_enabled"):
            return self._connect_with_retry(psycopg2)

        dsn = self._connect_kwargs()
        pool = self._pool(dsn)
        pool.warm(
            partial(psycopg2.connect, **dsn),
            self.full_details.get("connection_pooling_min_size", 0),
        )

        # Release any slot this wrapper still owns from a previous
        # ``create_connection`` call before acquiring a new one. The
        # query path calls ``make_connection`` again when
        # ``self._connection.closed`` becomes True mid-life (psycopg2
        # noticed the server-side close between two queries). Pre-fix
        # that path re-acquired a slot without releasing the old one,
        # so every flaky-network event silently drained one slot from
        # the pool. Under sustained instability the semaphore drained
        # to zero and every subsequent caller hung on ``acquire()``
        # for the full timeout then 503'd. Releasing the orphan first
        # keeps the per-wrapper invariant at ≤1 slot.
        self._release_pool_slot()

        if not pool.acquire(self._POOL_ACQUIRE_TIMEOUT):
            # Pool exhaustion is a capacity problem, not a query bug —
            # surface it as 503 so load balancers / clients can retry
            # instead of treating it as a 500 application fault.
            raise DatabaseUnavailableException(
                f"Connection pool exhausted: could not acquire a slot "
                f"within {self._POOL_ACQUIRE_TIMEOUT}s "
                f"(pool_size={pool.max_size})",
                retry_after=1,
            )
        self._permit_pool = pool

        try:
            connection, idle_since = pool.take()
            if connection is not None and self._revived(connection, idle_since):
                return connection
            return self._connect_with_retry(psycopg2)
        except BaseException:
            # ``BaseException``: an interrupt while probing or connecting
            # left the permit with a wrapper nobody would ever close.
            self._release_pool_slot()
            raise

    def _revived(self, connection, idle_since: float | None) -> bool:
        """Make a session leaving the pool ready for use; False (and closed)
        when it is not usable any more."""
        try:
            if connection.closed:
                return False
            if connection.info.transaction_status != 0:
                connection.rollback()
            connection.autocommit = True
            if self._needs_liveness_probe(idle_since):
                cursor = connection.cursor()
                try:
                    # SELECT 1 can raise if the server-side connection was
                    # closed without psycopg2 noticing (idle-in-transaction
                    # timeout, network drop); the cursor is released whether
                    # the probe succeeds or blows up.
                    cursor.execute("SELECT 1")
                finally:
                    with contextlib.suppress(Exception):
                        cursor.close()
            return True
        except Exception:
            with contextlib.suppress(Exception):
                connection.close()
            return False

    def _needs_liveness_probe(self, idle_since: float | None) -> bool:
        """Whether a connection leaving the pool must answer ``SELECT 1`` first:
        always, unless it was checked in within ``_POOL_PROBE_IDLE_SECONDS``."""
        window = self.full_details.get(
            "connection_pooling_probe_idle_seconds", _POOL_PROBE_IDLE_SECONDS
        )
        if idle_since is None or not window or window <= 0:
            return True
        return time.monotonic() - idle_since >= window

    def _connect_with_retry(self, psycopg2):
        """Create a new psycopg2 connection with exponential backoff on 'too many clients'."""
        last_err = None
        for attempt in range(self._MAX_CONNECT_RETRIES):
            try:
                return psycopg2.connect(**self._connect_kwargs())
            except psycopg2.OperationalError as e:
                last_err = e
                msg = str(e).lower()
                # Retry the transient, self-clearing outages with backoff:
                #  - "too many clients": a momentary pool spike.
                #  - SQLSTATE 57P03 / "starting up" / "in recovery": the node is
                #    up but not yet accepting queries during a primary FAILOVER
                #    or restart (typically 1-3s). Pre-fix every OperationalError
                #    except "too many clients" raised on the FIRST attempt, so a
                #    failover 503'd every request instead of riding out the
                #    short window.
                retriable = (
                    "too many clients" in msg
                    or "starting up" in msg
                    or "in recovery" in msg
                    or "cannot connect now" in msg
                    or getattr(e, "pgcode", None) == "57P03"
                )
                if retriable and attempt < self._MAX_CONNECT_RETRIES - 1:
                    wait = self._RETRY_BACKOFF_BASE * (2**attempt)
                    time.sleep(wait)
                    continue
                raise
        raise last_err

    @classmethod
    def get_default_query_grammar(cls):
        return PostgresGrammar

    @classmethod
    def get_default_platform(cls):
        return PostgresPlatform

    @classmethod
    def get_default_post_processor(cls):
        return PostgresPostProcessor

    def reconnect(self):
        """Close and re-create the connection.

        Uses close_connection() instead of raw _connection.close() so the
        pool semaphore slot is properly released before make_connection()
        acquires a new one.
        """
        self.transaction_level = 0
        self.close_connection()
        self.make_connection()

    def close_connection(self):
        if self._read_session is not None:
            # A leased session: handed back to its block, which ends it.
            self._return_read_session()
            return
        connection, self._connection = self._connection, None
        try:
            if connection is None:
                return
            if self._permit_pool is not None:
                # Back to the pool it was checked out of — the one that
                # issued this wrapper's permit.
                self._permit_pool.give_back(connection)
            else:
                # Not checked out of a pool (pooling off, or a session put
                # on the wrapper by hand): this wrapper pools nothing it did
                # not take from a pool.
                with contextlib.suppress(Exception):
                    connection.close()
        finally:
            self._release_pool_slot()

    @staticmethod
    def _validate_savepoint_name(name: str) -> None:
        """Guard against SQL injection in savepoint identifiers."""
        if not _SAVEPOINT_RE.match(name):
            raise InvalidArgumentException(
                f"Invalid savepoint name '{name}': "
                "must be alphanumeric/underscore, starting with a letter or underscore"
            )

    def savepoint(self, name):
        """Create a savepoint within the current transaction."""
        self._validate_savepoint_name(name)
        # No autocommit toggle here: a savepoint only exists inside an
        # already-open transaction (begin() sets autocommit=False for the
        # outer level), and psycopg2 raises "set_session cannot be used
        # inside a transaction" if autocommit is touched mid-transaction.
        cursor = self._connection.cursor()
        cursor.execute(f"SAVEPOINT {name}")
        cursor.close()
        self.transaction_level += 1

    def rollback_to_savepoint(self, name):
        """Rollback to a savepoint."""
        self._validate_savepoint_name(name)
        cursor = self._connection.cursor()
        cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
        cursor.close()
        self.transaction_level -= 1

    def release_savepoint(self, name):
        """Release a savepoint (commit it)."""
        self._validate_savepoint_name(name)
        cursor = self._connection.cursor()
        cursor.execute(f"RELEASE SAVEPOINT {name}")
        cursor.close()
        self.transaction_level -= 1

    def commit(self):
        """Transaction."""
        if self.transaction_level > 1:
            # Nested — release savepoint.
            # release_savepoint() already decrements transaction_level,
            # so we must NOT decrement again here.
            self.release_savepoint(f"sp_{self.transaction_level - 1}")
            return
        if self.transaction_level == 1:
            try:
                self._connection.commit()
                self._connection.autocommit = True
            finally:
                self.transaction_level -= 1
                self._snapshot_transaction = False

    def begin(self) -> Self:
        """Postgres Transaction with savepoint support for nesting."""
        if self.transaction_level > 0:
            # Nested transaction — use savepoint
            self.savepoint(f"sp_{self.transaction_level}")
            return self
        snapshot = scope_snapshot()
        if snapshot is not None:
            return self._begin_in_snapshot(snapshot)
        # The outer BEGIN may be a builder connection's first use.
        if not self._connection or self._connection.closed:
            self.make_connection()
        self._connection.autocommit = False
        self.transaction_level += 1
        return self

    def rollback(self):
        """Transaction with savepoint support for nesting.

        A session the server already ended (restart, failover, network drop,
        ``pg_terminate_backend``, ``idle_in_transaction_session_timeout``)
        took every open transaction level with it, so there is nothing to
        roll back. Calling ``rollback()`` on it raised ``InterfaceError:
        connection already closed``, which hid the real error and skipped the
        resolver's unpin-and-close. Each such rollback then kept a pool permit
        for the life of the process (db-chaos, 2026-11-12: three killed
        sessions exhausted a pool of three). Each level now unwinds without
        raising, and the last one drops the dead handle and frees its permit.
        """
        if self.transaction_level <= 0:
            return
        if self._session_is_dead():
            self._unwind_dead_session()
            return
        if self.transaction_level > 1:
            # Nested — rollback to savepoint.
            # rollback_to_savepoint() already decrements transaction_level,
            # so we must NOT decrement again here.
            try:
                self.rollback_to_savepoint(f"sp_{self.transaction_level - 1}")
            except Exception:
                if not self._session_is_dead():
                    raise
                self._unwind_dead_session()
            return
        if self.transaction_level == 1:
            self._snapshot_transaction = False
            try:
                self._connection.rollback()
                self._connection.autocommit = True
            except Exception:
                if not self._session_is_dead():
                    self.transaction_level -= 1
                    raise
                self._unwind_dead_session()
                return
            self.transaction_level -= 1

    def _session_is_dead(self) -> bool:
        """psycopg2 sets ``closed`` once the server side of the session is gone."""
        return self._connection is None or bool(self._connection.closed)

    def _unwind_dead_session(self) -> None:
        """Unwind one transaction level of a dead session; the last frees its permit."""
        self.transaction_level -= 1
        if self.transaction_level > 0:
            # The enclosing transaction's own rollback drops the handle.
            return
        self._snapshot_transaction = False
        dead, self._connection = self._connection, None
        if dead is not None:
            with contextlib.suppress(Exception):
                dead.close()
        self._release_pool_slot()

    def get_transaction_level(self):
        """Transaction."""
        return self.transaction_level

    def set_cursor(self):
        from psycopg2.extras import RealDictCursor  # local: heavy optional dep

        self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
        return self._cursor

    def query(self, query, bindings=(), results="*"):
        """
        Make the actual query that will reach the database and come back with a result.

        Arguments:
            query {string} -- A string query. This could be a qmarked string or a regular query.
            bindings {tuple} -- A tuple of bindings

        Keyword Arguments:
            results {str|1} -- If the results is equal to an asterisks it will call 'fetchAll'
                    else it will return 'fetchOne' and return a single record. (default: {"*"})

        Returns:
            dict|None -- Returns a dictionary of results or None
        """
        try:
            if not self._connection or self._connection.closed:
                if self.transaction_level > 0:
                    # The session of an OPEN transaction is gone and took the
                    # transaction with it. Reconnecting handed the rest of
                    # the block a fresh AUTOCOMMIT session while the level
                    # still said "in a transaction": earlier writes were
                    # lost, later ones committed one by one, and the block
                    # ended "successfully". All it took was one caller
                    # swallowing the statement that hit the dead session.
                    raise DatabaseUnavailableException(
                        "The database session of an open transaction was "
                        "lost; the transaction cannot continue.",
                        retry_after=1,
                    )
                self.make_connection()

            self.set_cursor()

            with self._cursor as cursor:
                if isinstance(query, list) and not self._dry:
                    for q in query:
                        self.statement(q, ())
                    return

                query = query.replace("'?'", "%s")
                self.statement(query, bindings)
                if results == 1:
                    if cursor.description is None:
                        return {}
                    return dict(cursor.fetchone() or {})
                else:
                    # ``cursor.description`` is the canonical has-rowset
                    # signal (SELECT, any RETURNING clause) — DDL like
                    # CREATE MATERIALIZED VIEW reports "SELECT N" in its
                    # status message with no rowset, and INSERT ...
                    # RETURNING reports "INSERT 0 1" WITH one, so the old
                    # statusmessage check misrouted both.
                    if cursor.description is not None:
                        return cursor.fetchall()
                    # Non-result statements (UPDATE/DELETE/INSERT without
                    # RETURNING): surface the affected row count —
                    # ``DB.statement()`` and ``delete()`` callers need it
                    # (chunked prune loops used to stall on the old ``{}``).
                    # DDL reports -1; normalize to 0.
                    return max(cursor.rowcount, 0)
        except Exception as e:
            # Distinguish "DB is down / connection lost" from "bad query"
            # so the exception handler can return 503 (retryable) instead
            # of 500 (application fault). psycopg2.OperationalError covers
            # both: a) network/connect failure, b) the connection dropped
            # mid-query. Pool exhaustion already raises
            # ``DatabaseUnavailableException`` directly in
            # ``create_connection``; that path is re-raised here unchanged.
            if isinstance(e, DatabaseUnavailableException):
                raise
            try:
                import psycopg2  # local: heavy optional dep
            except ModuleNotFoundError:
                psycopg2 = None  # type: ignore[assignment]
            if psycopg2 is not None and isinstance(e, psycopg2.OperationalError):
                # A serialization failure / deadlock is an OperationalError
                # SUBCLASS, so it must be classified BEFORE the outage branch.
                # It is not an outage: the database is healthy and answering,
                # it just refused this transaction so the caller can replay
                # it. Classifying it as ``DatabaseUnavailableException`` put a
                # write conflict in the outage taxonomy — a 503 telling the
                # client the database is down, and an on-call page for a
                # database that never faltered. ``atomic(attempts=N)`` still
                # retried it either way, because ``Integrity.sqlstate_of``
                # unwraps the cause chain to find SQLSTATE 40001/40P01; what
                # was wrong is what everyone ELSE was told.
                if isinstance(e, psycopg2.extensions.TransactionRollbackError):
                    raise QueryException(str(e)) from e
                raise DatabaseUnavailableException(str(e), retry_after=1) from e
            raise QueryException(str(e)) from e
        finally:
            if self.get_transaction_level() <= 0:
                self.open = 0
                self.close_connection()
