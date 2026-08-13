"""Canonical definition of ``BlueprintExecutor``."""

from __future__ import annotations


class BlueprintExecutor:
    """Wrapper that executes Blueprint SQL after context manager exits"""

    def __init__(self, blueprint, schema):
        self.blueprint = blueprint
        self.schema = schema

    def __enter__(self):
        return self.blueprint.__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # First let blueprint store its SQL
        result = self.blueprint.__exit__(exc_type, exc_value, exc_traceback)

        # If no exception, execute the SQL using schema's query executor
        if exc_type is None:
            sql_statements = self.blueprint.get_sql()
            if isinstance(sql_statements, list):
                for sql in sql_statements:
                    if sql and sql.strip():
                        self.schema.query_executor.execute_query(sql.strip())
            elif sql_statements and sql_statements.strip():
                self.schema.query_executor.execute_query(sql_statements.strip())

        return result
