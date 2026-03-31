"""
BetweenExpression - Simple expression class for BETWEEN conditions

Handles BETWEEN clauses in a clean, simple way.
Compatible with BaseGrammar.process_wheres which reads .equality,
.value, .value_type, .raw, .keyword, .low, .high, and .column.
"""


class BetweenExpression:
    """Represents a BETWEEN / NOT BETWEEN condition in SQL."""

    def __init__(self, column: str, min_value, max_value, not_between: bool = False, keyword=None):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.not_between = not_between

        self.low = min_value
        self.high = max_value
        self.equality = "NOT BETWEEN" if not_between else "BETWEEN"
        self.value = None
        self.value_type = "BETWEEN"
        self.raw = False
        self.keyword = keyword
        self.bindings = ()

    def __str__(self) -> str:
        return f"{self.column} {self.equality} {self.min_value} AND {self.max_value}"

    def __repr__(self) -> str:
        return f"BetweenExpression(column='{self.column}', low={self.min_value!r}, high={self.max_value!r}, not_between={self.not_between})"

    def to_sql(self) -> str:
        return str(self)
