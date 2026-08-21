"""Read-only PostgreSQL schema inspection for ``schema:check``."""

from __future__ import annotations


class _LiveSchemaInspection:
    @staticmethod
    def _target_schema(live_schema, schema_name) -> str:
        return schema_name or live_schema.get_schema() or "public"

    @staticmethod
    def _sql_literal(value: str) -> str:
        return str(value).replace("'", "''")

    def tables(self, live_schema, schema_name) -> dict[str, dict]:
        target = self._sql_literal(self._target_schema(live_schema, schema_name))
        sql = (
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            f"WHERE table_schema = '{target}' "
            "ORDER BY table_name, ordinal_position"
        )
        rows = live_schema.query_executor.get_query_result(sql) or []
        tables: dict[str, dict] = {}
        for row in rows:
            table_name = row["table_name"]
            tables.setdefault(table_name, {})[row["column_name"]] = {
                "data_type": (row["data_type"] or "").lower(),
                "is_nullable": (row["is_nullable"] or "").upper() == "YES",
                "max_length": row.get("character_maximum_length"),
                # Numeric WIDTH is the same class of silent data loss as a
                # too-narrow varchar, and it was invisible here: a live
                # numeric(6,4) under a model declaring numeric(20,18) coerces
                # every write and, where a CHECK re-derives a value from the
                # stored column, rejects the row outright.
                "numeric_precision": row.get("numeric_precision"),
                "numeric_scale": row.get("numeric_scale"),
            }
        return tables

    def checks(self, live_schema, schema_name) -> dict[str, set[str]]:
        target = self._sql_literal(self._target_schema(live_schema, schema_name))
        sql = (
            "SELECT c.relname AS table_name, con.conname AS constraint_name "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{target}' "
            "AND con.contype = 'c' "
            "ORDER BY c.relname, con.conname"
        )
        rows = live_schema.query_executor.get_query_result(sql) or []
        checks: dict[str, set[str]] = {}
        for row in rows:
            checks.setdefault(row["table_name"], set()).add(row["constraint_name"])
        return checks

    def indexes(self, live_schema, schema_name) -> dict[str, set[str]]:
        target = self._sql_literal(self._target_schema(live_schema, schema_name))
        sql = (
            "SELECT tablename AS table_name, indexname AS index_name "
            "FROM pg_indexes "
            f"WHERE schemaname = '{target}' "
            "ORDER BY tablename, indexname"
        )
        rows = live_schema.query_executor.get_query_result(sql) or []
        indexes: dict[str, set[str]] = {}
        for row in rows:
            indexes.setdefault(row["table_name"], set()).add(row["index_name"])
        return indexes

    def constraint_indexes(self, live_schema, schema_name) -> dict[str, set[str]]:
        target = self._sql_literal(self._target_schema(live_schema, schema_name))
        sql = (
            "SELECT c.relname AS table_name, i.relname AS index_name "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_class i ON i.oid = con.conindid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{target}' "
            "AND con.conindid <> 0 "
            "ORDER BY c.relname, i.relname"
        )
        rows = live_schema.query_executor.get_query_result(sql) or []
        owned: dict[str, set[str]] = {}
        for row in rows:
            owned.setdefault(row["table_name"], set()).add(row["index_name"])
        return owned


_LIVE_SCHEMA_INSPECTION = _LiveSchemaInspection()
