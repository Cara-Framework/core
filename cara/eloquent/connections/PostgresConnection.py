from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

import contextlib
import re
import threading
import time

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

_SAVEPOINT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_pool_lock = threading.Lock()
CONNECTION_POOL = []
_pool_initialized = False
_pool_semaphore = None


class PostgresConnection(BaseConnection):
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
        self.connection_pool_size = self.full_details.get(
            "connection_pooling_max_size", 100
        )
        self.options = options or {}
        self._cursor = None
        self.transaction_level = 0
        self.open = 0
        self.schema = None
        if name:
            self.name = name

    def make_connection(self):
        """This sets the connection on the connection class."""
        try:
            import psycopg2  # noqa F401
        except ModuleNotFoundError:
            raise DriverNotFoundException(
                "You must have the 'psycopg2' package installed to make a connection to Postgres. Please install it using 'pip install psycopg2-binary'"
            )

        if self.has_global_connection():
            return self.get_global_connection()

        self._connection = self.create_connection()

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
        except (OSError, RuntimeError, AttributeError):
            with contextlib.suppress(OSError, RuntimeError, AttributeError):
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

    def _ensure_pool_initialized(self):
        global _pool_initialized, _pool_semaphore
        if _pool_initialized:
            return
        with _pool_lock:
            if _pool_initialized:
                return
            _pool_semaphore = threading.Semaphore(self.connection_pool_size)
            min_size = self.full_details.get("connection_pooling_min_size", 0)
            if min_size:
                import psycopg2

                for _ in range(min_size):
                    try:
                        conn = psycopg2.connect(**self._connect_kwargs())
                        conn.autocommit = True
                        CONNECTION_POOL.append(conn)
                    except Exception as warm_err:
                        # A cold DB at boot leaves the pool with 0 pre-warmed
                        # connections (lazy connect recovers later, so no hang/
                        # leak) — but log it, or a boot-against-a-dead-DB is
                        # completely invisible.
                        import logging

                        logging.getLogger("cara.database.pool").warning(
                            "Pool warm-up connect failed (%s pre-warmed); "
                            "falling back to lazy connect: %s",
                            len(CONNECTION_POOL),
                            warm_err,
                        )
                        break
            _pool_initialized = True

    _POOL_ACQUIRE_TIMEOUT = 30

    def create_connection(self):
        import psycopg2

        if not self.full_details.get("connection_pooling_enabled"):
            return self._connect_with_retry(psycopg2)

        self._ensure_pool_initialized()

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
        if getattr(self, "_pool_slot_acquired", False) and _pool_semaphore is not None:
            _pool_semaphore.release()
            self._pool_slot_acquired = False

        acquired = _pool_semaphore.acquire(timeout=self._POOL_ACQUIRE_TIMEOUT)
        if not acquired:
            # Pool exhaustion is a capacity problem, not a query bug —
            # surface it as 503 so load balancers / clients can retry
            # instead of treating it as a 500 application fault.
            raise DatabaseUnavailableException(
                f"Connection pool exhausted: could not acquire a slot "
                f"within {self._POOL_ACQUIRE_TIMEOUT}s "
                f"(pool_size={self.connection_pool_size})",
                retry_after=1,
            )
        self._pool_slot_acquired = True

        connection = None
        with _pool_lock:
            if CONNECTION_POOL:
                connection = CONNECTION_POOL.pop()

        if connection:
            try:
                if connection.closed:
                    connection = None
                else:
                    if connection.info.transaction_status != 0:
                        connection.rollback()
                    connection.autocommit = True
                    cursor = connection.cursor()
                    try:
                        # SELECT 1 can raise if the server-side
                        # connection was closed without psycopg2 noticing
                        # (idle-in-transaction timeout, network drop).
                        # The bare execute()→close() pattern leaked the
                        # cursor on that path — wrap in try/finally so
                        # the cursor is released whether the probe
                        # succeeds or blows up.
                        cursor.execute("SELECT 1")
                    finally:
                        with contextlib.suppress(OSError, RuntimeError, AttributeError):
                            cursor.close()
            except (OSError, RuntimeError, AttributeError):
                with contextlib.suppress(OSError, RuntimeError, AttributeError):
                    connection.close()
                connection = None

        if not connection:
            try:
                connection = self._connect_with_retry(psycopg2)
            except (OSError, RuntimeError, AttributeError):
                _pool_semaphore.release()
                self._pool_slot_acquired = False
                raise

        return connection

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

    def get_database_name(self):
        return self.database

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
        if self._connection is None:
            if (
                getattr(self, "_pool_slot_acquired", False)
                and _pool_semaphore is not None
            ):
                _pool_semaphore.release()
                self._pool_slot_acquired = False
            return

        if self.full_details.get("connection_pooling_enabled"):
            with _pool_lock:
                if len(CONNECTION_POOL) < self.connection_pool_size:
                    try:
                        if not self._connection.closed:
                            if self._connection.info.transaction_status != 0:
                                self._connection.rollback()
                            self._connection.autocommit = True
                            CONNECTION_POOL.append(self._connection)
                        # else: already closed, discard
                    except (OSError, RuntimeError, AttributeError):
                        with contextlib.suppress(OSError, RuntimeError, AttributeError):
                            self._connection.close()
                else:
                    with contextlib.suppress(OSError, RuntimeError, AttributeError):
                        self._connection.close()

            if (
                getattr(self, "_pool_slot_acquired", False)
                and _pool_semaphore is not None
            ):
                _pool_semaphore.release()
                self._pool_slot_acquired = False
        else:
            with contextlib.suppress(OSError, RuntimeError, AttributeError):
                self._connection.close()

        self._connection = None

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

    def begin(self) -> Self:
        """Postgres Transaction with savepoint support for nesting."""
        if self.transaction_level > 0:
            # Nested transaction — use savepoint
            self.savepoint(f"sp_{self.transaction_level}")
            return self
        self._connection.autocommit = False
        self.transaction_level += 1
        return self

    def rollback(self):
        """Transaction with savepoint support for nesting."""
        if self.transaction_level <= 0:
            return
        if self.transaction_level > 1:
            # Nested — rollback to savepoint.
            # rollback_to_savepoint() already decrements transaction_level,
            # so we must NOT decrement again here.
            self.rollback_to_savepoint(f"sp_{self.transaction_level - 1}")
            return
        if self.transaction_level == 1:
            try:
                self._connection.rollback()
                self._connection.autocommit = True
            finally:
                self.transaction_level -= 1

    def get_transaction_level(self):
        """Transaction."""
        return self.transaction_level

    def set_cursor(self):
        from psycopg2.extras import RealDictCursor

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
                import psycopg2  # local import — psycopg2 may not be installed
            except ModuleNotFoundError:
                psycopg2 = None  # type: ignore[assignment]
            if psycopg2 is not None and isinstance(e, psycopg2.OperationalError):
                raise DatabaseUnavailableException(str(e), retry_after=1) from e
            raise QueryException(str(e)) from e
        finally:
            if self.get_transaction_level() <= 0:
                self.open = 0
                self.close_connection()
