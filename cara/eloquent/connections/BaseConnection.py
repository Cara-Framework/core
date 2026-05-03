from timeit import default_timer as timer


class BaseConnection:
    """
    Single Responsibility: Base connection functionality
    No direct dependency on ConnectionResolver - avoids circular dependencies
    """

    _connection = None
    _cursor = None
    _dry = False
    # Bumped from 500ms — at 500ms a cold-connection first call against
    # a freshly-restored Postgres (FK validation hitting cold pages,
    # session settings, statement parsing on a fresh prepared-statement
    # cache) routinely lands at 500-700ms even for trivial inserts. The
    # warning becomes pure noise at startup. 750ms still catches every
    # actionable slow query (anything beyond cold-cache jitter is real).
    SLOW_QUERY_THRESHOLD_MS = 750

    def dry(self):
        self._dry = True
        return self

    def set_schema(self, schema):
        self.schema = schema
        return self

    def log(
        self,
        query,
        bindings,
        query_time=0,
    ):
        from cara.facades import Log

        Log.database(f"Running query {query}, {bindings}. Executed in {query_time}ms")

    @staticmethod
    def _normalize_query_for_log(query: str, max_len: int = 1000) -> str:
        """Collapse whitespace and truncate so a log line is one-pretty-line.

        Multi-line SQL blobs (the typical INSERT ... VALUES (...) ON CONFLICT
        DO UPDATE SET ... pattern) wrap awkwardly in log aggregators; one
        clean line keeps the slow-query warning grep-friendly. Truncate at
        a wider window than before (was 500) so the ON CONFLICT body and
        the closing of the VALUES tuple aren't sliced off.
        """
        compact = " ".join(query.split())
        if len(compact) <= max_len:
            return compact
        return compact[:max_len] + f"… [+{len(compact) - max_len}c]"

    def statement(self, query, bindings=()):
        """
        Wrapper around calling the cursor query. Helpful for logging output.

        Args:
            query (string): The query to execute on the cursor
            bindings (tuple, optional): Tuple of query bindings. Defaults to ().
        """
        start = timer()
        if not self._cursor:
            raise AttributeError(
                f"Must set the _cursor attribute on the {self.__class__.__name__} class before calling the 'statement' method."
            )

        # psycopg2 / some DB-API drivers still scan the query for `%s`
        # placeholders when an (even empty) bindings sequence is passed.
        # Statements with literal `%` (e.g. PL/pgSQL `FORMAT '%I'`, `TO_CHAR(..., 'MM')`)
        # then blow up with IndexError. Pass None when we have no bindings so
        # the driver skips parameter parsing entirely.
        self._cursor.execute(query, bindings if bindings else None)
        elapsed_ms = (timer() - start) * 1000  # Convert to ms
        elapsed_formatted = "{:.2f}".format(elapsed_ms / 1000)

        # Slow query detection
        threshold = (self.full_details or {}).get(
            "slow_query_threshold_ms", self.SLOW_QUERY_THRESHOLD_MS
        )
        if elapsed_ms >= threshold:
            from cara.facades import Log

            # Annotate with binding count + transaction level so an
            # operator can tell at a glance whether this was a fat
            # batched insert (binding count high), a single statement
            # inside a long-running tx (level > 1), or a one-shot.
            # Bindings themselves are NOT logged — they may carry PII.
            n_bindings = len(bindings) if bindings else 0
            tx_level = self.get_transaction_level() if hasattr(self, "get_transaction_level") else 0
            Log.warning(
                f"SLOW QUERY ({elapsed_ms:.0f}ms, "
                f"params={n_bindings}, tx={tx_level}): "
                f"{self._normalize_query_for_log(query)}",
                category="slow_query",
            )

        # Log query if either connection-specific log_queries is True
        # or if LOG_DB_QUERIES is enabled via logging config
        if self.full_details and self.full_details.get("log_queries", False):
            self.log(query, bindings, query_time=elapsed_formatted)

    def has_global_connection(self):
        """Check if there's a global connection - removed circular dependency"""
        # This method is now handled by ConnectionResolver externally
        # Each connection instance manages its own state
        return hasattr(self, "_is_global") and self._is_global

    def get_global_connection(self):
        """Get global connection - removed circular dependency"""
        # This is now handled by ConnectionResolver externally
        # Return self if this is the global connection
        if self.has_global_connection():
            return self
        return None

    def set_as_global(self, is_global=True):
        """Mark this connection as global - avoids circular dependency"""
        self._is_global = is_global
        return self

    def enable_query_log(self):
        """Enable query logging for this connection instance."""
        if not self.full_details:
            self.full_details = {}
        self.full_details["log_queries"] = True

    def disable_query_log(self):
        """Disable query logging for this connection instance."""
        if not self.full_details:
            self.full_details = {}
        self.full_details["log_queries"] = False

    def format_cursor_results(self, cursor_result):
        return cursor_result

    def set_cursor(self):
        self._cursor = self._connection.cursor()
        return self

    def select_many(self, query, bindings, amount):
        self.set_cursor()
        self.statement(query, bindings)
        if not self.open:
            self.make_connection()

        try:
            result = self.format_cursor_results(self._cursor.fetchmany(amount))
            while result:
                yield result
                result = self.format_cursor_results(self._cursor.fetchmany(amount))
        finally:
            # Ensure cursor/connection cleanup even if the caller
            # abandons the generator before it is fully consumed.
            if self.get_transaction_level() <= 0:
                self.open = 0
                self.close_connection()

    def enable_disable_foreign_keys(self):
        foreign_keys = self.full_details.get("foreign_keys")
        platform = self.get_default_platform()()

        if foreign_keys:
            self._connection.execute(platform.enable_foreign_key_constraints())
        elif foreign_keys is not None:
            self._connection.execute(platform.disable_foreign_key_constraints())
