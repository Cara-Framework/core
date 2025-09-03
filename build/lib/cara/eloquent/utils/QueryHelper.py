"""
QueryHelper - Utility for query operations
"""


class QueryHelper:
    """Helper for query building operations."""

    @staticmethod
    def sanitize_table_name(table_name: str) -> str:
        """Sanitize table name."""
        return table_name.strip().replace(" ", "_").lower()

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """Quote database identifier."""
        return f"`{identifier}`"

    @staticmethod
    def build_where_clause(column: str, operator: str, value) -> str:
        """Build a WHERE clause."""
        return f"{column} {operator} ?"

    @staticmethod
    def escape_like_value(value: str) -> str:
        """Escape LIKE value."""
        return value.replace("%", r"\%").replace("_", r"\_")

    @staticmethod
    def build_limit_clause(limit: int, offset: int = None) -> str:
        """Build LIMIT clause."""
        if offset:
            return f"LIMIT {offset}, {limit}"
        return f"LIMIT {limit}"
