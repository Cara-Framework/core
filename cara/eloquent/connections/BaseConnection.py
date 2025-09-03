from timeit import default_timer as timer


class BaseConnection:
    """
    Single Responsibility: Base connection functionality
    No direct dependency on ConnectionResolver - avoids circular dependencies
    """

    _connection = None
    _cursor = None
    _dry = False

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

        self._cursor.execute(query, bindings)
        end = "{:.2f}".format(timer() - start)

        # Log query if either connection-specific log_queries is True
        # or if LOG_DB_QUERIES is enabled via logging config
        if self.full_details and self.full_details.get("log_queries", False):
            self.log(query, bindings, query_time=end)

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
        self.statement(query)
        if not self.open:
            self.make_connection()

        result = self.format_cursor_results(self._cursor.fetchmany(amount))
        while result:
            yield result

            result = self.format_cursor_results(self._cursor.fetchmany(amount))

    def enable_disable_foreign_keys(self):
        foreign_keys = self.full_details.get("foreign_keys")
        platform = self.get_default_platform()()

        if foreign_keys:
            self._connection.execute(platform.enable_foreign_key_constraints())
        elif foreign_keys is not None:
            self._connection.execute(platform.disable_foreign_key_constraints())
