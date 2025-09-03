"""
WhereClause - Simple WHERE clause component
"""

class WhereClause:
    """Simple WHERE clause representation."""
    
    def __init__(self, column: str, operator: str, value, boolean: str = "AND"):
        self.column = column
        self.operator = operator
        self.value = value
        self.boolean = boolean
    
    def __str__(self) -> str:
        return f"{self.column} {self.operator} {self.value}"
    
    def to_sql(self) -> str:
        return str(self)

