import re
import threading
import time

from cara.exceptions import DriverNotFoundException, QueryException

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
        self.connection_pool_size = self.full_details.get("connection_pooling_max_size", 100)
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

        self._connection.autocommit = True

        self.enable_disable_foreign_keys()

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
        schema = self.schema or self.full_details.get("schema")
        if schema:
            kw["options"] = f"-c search_path={schema}"
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
                    except Exception:
                        break
            _pool_initialized = True

    _POOL_ACQUIRE_TIMEOUT = 30

    def create_connection(self):
        import psycopg2

        if not self.full_details.get("connection_pooling_enabled"):
            return self._connect_with_retry(psycopg2)

        self._ensure_pool_initialized()

        acquired = _pool_semaphore.acquire(timeout=self._POOL_ACQUIRE_TIMEOUT)
        if not acquired:
            raise psycopg2.OperationalError(
                f"Connection pool exhausted: could not acquire a slot "
                f"within {self._POOL_ACQUIRE_TIMEOUT}s "
                f"(pool_size={self.connection_pool_size})"
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
                    cursor.execute("SELECT 1")
                    cursor.close()
            except Exception:
                try:
                    connection.close()
                except Exception:
                    pass
                connection = None

        if not connection:
            try:
                connection = self._connect_with_retry(psycopg2)
            except Exception:
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
                if "too many clients" in str(e) and attempt < self._MAX_CONNECT_RETRIES - 1:
                    wait = self._RETRY_BACKOFF_BASE * (2 ** attempt)
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
            if getattr(self, "_pool_slot_acquired", False) and _pool_semaphore is not None:
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
                    except Exception:
                        try:
                            self._connection.close()
                        except Exception:
                            pass
                else:
                    try:
                        self._connection.close()
                    except Exception:
                        pass

            if getattr(self, "_pool_slot_acquired", False) and _pool_semaphore is not None:
                _pool_semaphore.release()
                self._pool_slot_acquired = False
        else:
            try:
                self._connection.close()
            except Exception:
                pass

        self._connection = None

    @staticmethod
    def _validate_savepoint_name(name: str) -> None:
        """Guard against SQL injection in savepoint identifiers."""
        if not _SAVEPOINT_RE.match(name):
            raise ValueError(
                f"Invalid savepoint name '{name}': "
                "must be alphanumeric/underscore, starting with a letter or underscore"
            )

    def savepoint(self, name):
        """Create a savepoint within the current transaction."""
        self._validate_savepoint_name(name)
        self._connection.autocommit = False
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

    def begin(self):
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
                    # `cursor.description` is only populated for result-bearing
                    # statements (SELECT, RETURNING, …). DDL such as
                    # CREATE MATERIALIZED VIEW reports a status message like
                    # "SELECT N" even though it yields no rowset, which would
                    # previously blow up in `fetchall()` with "no results to
                    # fetch". Guarding on description keeps the behaviour safe.
                    if "SELECT" in cursor.statusmessage and cursor.description is not None:
                        return cursor.fetchall()
                    return {}
        except Exception as e:
            raise QueryException(str(e)) from e
        finally:
            if self.get_transaction_level() <= 0:
                self.open = 0
                self.close_connection()
                # self._connection.close()
