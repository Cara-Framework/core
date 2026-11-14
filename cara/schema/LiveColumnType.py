"""A live column's full type, as SQL that recreates it.

Used where a reverse statement must bring a column back as it WAS: the
reverse of a ``DROP COLUMN`` and of a widening. Rebuilt from
``information_schema.data_type`` alone, ``numeric(12,2) NOT NULL DEFAULT 0``
came back as unbounded ``NUMERIC``, nullable and with no default, although
the live read already collects the precision, the scale and the default.
"""

from __future__ import annotations


def live_type_sql(live: dict) -> str:
    """The live column's type: width, precision, array element, enum."""
    base = (live.get("data_type") or "text").upper()
    max_length = live.get("max_length")
    udt = live.get("udt_name")
    if max_length and base in {"CHARACTER VARYING", "VARCHAR"}:
        return f"VARCHAR({max_length})"
    if max_length and base in {"CHARACTER", "CHAR"}:
        return f"CHAR({max_length})"
    precision, scale = live.get("numeric_precision"), live.get("numeric_scale")
    if base == "NUMERIC" and precision is not None and scale is not None:
        return f"NUMERIC({precision},{scale})"
    if base == "ARRAY" and udt and udt.startswith("_"):
        return f'"{udt[1:]}"[]'
    if base == "USER-DEFINED" and udt:
        return f'"{udt}"'
    return base


def readd_column_sql(table: str, name: str, live: dict) -> str:
    """The reverse of a ``DROP COLUMN``: the column as it was, empty.

    NOT NULL returns only with a default to give the existing rows; a
    sequence default does not, because the sequence went with the column.
    """
    default = live.get("default")
    if default and str(default).lower().startswith("nextval("):
        default = None
    sql = f'ALTER TABLE "{table}" ADD COLUMN "{name}" {live_type_sql(live)}'
    if default is not None:
        sql += f" DEFAULT {default}"
        if not live.get("is_nullable", True):
            sql += " NOT NULL"
    return sql


__all__ = ["live_type_sql", "readd_column_sql"]
