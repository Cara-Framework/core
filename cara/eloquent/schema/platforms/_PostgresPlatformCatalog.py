"""Catalogue reads of ``PostgresPlatform``: does a table exist, which tables
are there, what columns does one have.

**Which schema.** A caller that names a schema is asked about that schema.
One that names none is asked about the schema the SESSION resolves names
to — never a hard-coded ``public``. The framework's own statements are
unqualified and follow the search_path, so on a role or database whose
search_path is not ``public`` the two disagreed: the migration tracker read
and wrote a ledger that ``has_table`` reported missing, ``get_all_tables``
listed another schema's tables for ``DROP TABLE`` statements that resolved
here, and a model's columns read as empty.
"""

from __future__ import annotations

from ...schema import Schema
from ..Table import Table


def _schema_sql(platform, schema) -> str:
    """The schema a catalogue read filters on, as SQL: the named one, or the
    session's own (the first schema of its search_path that exists)."""
    if not schema:
        return "current_schema()"
    return f"'{platform._validate_identifier(schema, 'schema name')}'"


def _compile_table_exists(self, table, database=None, schema=None):
    """With no schema named, ``to_regclass`` resolves the name exactly as an
    unqualified reference does: through the whole search_path, NULL when
    nothing is there. The relkind list is ``information_schema.tables``' own,
    so an index or a sequence of that name still is not a table.
    """
    table = self._validate_identifier(table, "table name")
    if not schema:
        # Each part quoted on its own: a validated name may be qualified.
        name = ".".join(f'"{part}"' for part in table.split("."))
        return (
            "SELECT c.relname AS table_name FROM pg_catalog.pg_class c "
            f"WHERE c.oid = to_regclass('{name}') "
            "AND c.relkind IN ('r', 'p', 'v', 'f')"
        )
    schema = self._validate_identifier(schema, "schema name")
    return f"SELECT * from information_schema.tables where table_name='{table}' AND table_schema = '{schema}'"


def _compile_column_exists(self, table, column):
    table = self._validate_identifier(table, "table name")
    column = self._validate_identifier(column, "column name")
    return f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' and column_name='{column}'"


def _compile_get_all_tables(self, database=None, schema=None):
    database = (
        self._validate_identifier(database, "database name") if database else "postgres"
    )
    return (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = {_schema_sql(self, schema)} AND table_catalog = '{database}'"
    )


def _get_current_schema(self, connection, table_name, schema=None):
    self._validate_identifier(table_name, "table name")
    if schema:
        self._validate_identifier(schema, "schema name")
        sql = self.table_information_string().format(table=table_name, schema=schema)
    else:
        sql = (
            "SELECT * FROM information_schema.columns "
            f"WHERE table_schema = current_schema() AND table_name = '{table_name}'"
        )

    reversed_type_map = {v: k for k, v in self.type_map.items()}
    reversed_type_map.update(self.table_info_map)
    table = Table(table_name)

    result = connection.query(sql, ())
    for column in result:
        column_type = reversed_type_map.get(column["data_type"].upper())

        # find length
        if column.get("character_maximum_length", None):
            length = column.get("character_maximum_length")
        elif column.get("numeric_precision", None):
            length = column.get("numeric_precision")
        elif column.get("datetime_precision", None):
            length = column.get("datetime_precision")
        else:
            length = None

        # find default
        default_val = column.get("dflt_value", "") or column.get("column_default", "")
        if default_val and default_val.startswith("nextval"):
            table.set_primary_key(column["column_name"])
            default_val = None

        table.add_column(
            column["column_name"],
            column_type,
            default=default_val,
            column_python_type=Schema._type_hints_map.get(column_type, str),
            length=length,
        )

    return table
