import threading

from cara.exceptions import DriverNotFoundException, QueryException

from ..query.grammars import MySQLGrammar
from ..query.processors import MySQLPostProcessor
from ..schema.platforms import MySQLPlatform
from .BaseConnection import BaseConnection

# Per-target connection pools keyed by (host, port, database, user) so that
# different tenants / databases never share raw connection handles.
CONNECTION_POOLS: dict = {}
_POOL_LOCK = threading.Lock()


class MySQLConnection(BaseConnection):
    """MYSQL Connection class."""

    name = "mysql"
    _dry = False

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
        self.port = port
        if str(port).isdigit():
            self.port = int(self.port)
        self.database = database

        self.user = user
        self.password = password
        self.prefix = prefix
        self.full_details = full_details or {}
        self.connection_pool_size = self.full_details.get("connection_pooling_max_size", 100)
        self.options = options or {}
        self._cursor = None
        self.open = 0
        self.transaction_level = 0
        if name:
            self.name = name

    def _pool_key(self) -> tuple:
        return (self.host, self.port, self.database, self.user)

    def make_connection(self):
        """This sets the connection on the connection class."""

        if self._dry:
            return

        if self.has_global_connection():
            return self.get_global_connection()

        # Check if there is an available connection in the pool
        self._connection = self.create_connection()
        self.enable_disable_foreign_keys()

        return self

    def close_connection(self):
        if self._connection is None:
            self.open = 0
            return

        if self.full_details.get("connection_pooling_enabled"):
            key = self._pool_key()
            with _POOL_LOCK:
                pool = CONNECTION_POOLS.setdefault(key, [])
                if len(pool) < self.connection_pool_size:
                    pool.append(self._connection)
                else:
                    try:
                        # Bypass any monkey-patch — we want the real driver close.
                        type(self._connection).close(self._connection)
                    except Exception:
                        pass
        else:
            try:
                type(self._connection).close(self._connection)
            except Exception:
                pass

        self.open = 0
        self._connection = None

    def create_connection(self, autocommit=True):
        try:
            import pymysql
        except ModuleNotFoundError:
            raise DriverNotFoundException(
                "You must have the 'pymysql' package "
                "installed to make a connection to MySQL. "
                "Please install it using 'pip install pymysql'"
            )
        import pendulum
        import pymysql.converters

        pymysql.converters.conversions[pendulum.DateTime] = (
            pymysql.converters.escape_datetime
        )

        pooling_enabled = bool(self.full_details.get("connection_pooling_enabled"))
        key = self._pool_key()
        initialize_size = self.full_details.get("connection_pooling_min_size")

        def _new_connection():
            return pymysql.connect(
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=autocommit,
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                database=self.database,
                **self.options,
            )

        connection = None

        if pooling_enabled:
            with _POOL_LOCK:
                pool = CONNECTION_POOLS.setdefault(key, [])

                # Warm up the pool if requested and it's below the min size.
                if initialize_size and len(pool) < initialize_size:
                    missing = initialize_size - len(pool)
                    for _ in range(missing):
                        pool.append(_new_connection())

                if pool:
                    connection = pool.pop()

        if connection is None:
            connection = _new_connection()

        self.open = 1

        return connection

    def reconnect(self):
        self._connection.connect()
        return self

    @classmethod
    def get_default_query_grammar(cls):
        return MySQLGrammar

    @classmethod
    def get_default_platform(cls):
        return MySQLPlatform

    @classmethod
    def get_default_post_processor(cls):
        return MySQLPostProcessor

    def get_database_name(self):
        return self.database

    def commit(self):
        """Transaction."""
        self._connection.commit()
        self.transaction_level -= 1
        if self.get_transaction_level() <= 0:
            self.open = 0
            try:
                self._connection.close()
            except Exception:
                pass

    def dry(self):
        """Transaction."""
        self._dry = True
        return self

    def begin(self):
        """Mysql Transaction."""
        self._connection.begin()
        self.transaction_level += 1
        return self

    def rollback(self):
        """Transaction."""
        self._connection.rollback()
        self.transaction_level -= 1
        if self.get_transaction_level() <= 0:
            self.open = 0
            try:
                self._connection.close()
            except Exception:
                pass

    def get_transaction_level(self):
        """Transaction."""
        return self.transaction_level

    def get_cursor(self):
        return self._cursor

    def query(self, query, bindings=(), results="*"):
        """
        Make the actual query that will reach the database and come back with a result.

        Arguments:
            query {string} -- A string query.
            This could be a qmarked string or a regular query.
            bindings {tuple} -- A tuple of bindings

        Keyword Arguments:
            results {str|1} -- If the results is equal to an
            asterisks it will call 'fetchAll'
            else it will return 'fetchOne' and
            return a single record. (default: {"*"})

        Returns:
            dict|None -- Returns a dictionary of results or None
        """

        if self._dry:
            return {}

        if not self.open:
            if self._connection is None:
                self._connection = self.create_connection()

            self._connection.connect()

        self._cursor = self._connection.cursor()

        try:
            with self._cursor as cursor:
                if isinstance(query, list):
                    for q in query:
                        q = q.replace("'?'", "%s")
                        self.statement(q, ())
                    return

                query = query.replace("'?'", "%s")
                self.statement(query, bindings)
                if results == 1:
                    return self.format_cursor_results(cursor.fetchone())
                else:
                    return self.format_cursor_results(cursor.fetchall())
        except Exception as e:
            raise QueryException(str(e)) from e
        finally:
            self._cursor.close()
            if self.get_transaction_level() <= 0:
                self.open = 0
                self._connection.close()
