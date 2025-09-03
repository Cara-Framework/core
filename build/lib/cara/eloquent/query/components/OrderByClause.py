"""
OrderByClause - Simple ORDER BY clause component
"""

class OrderByClause:
    """Simple ORDER BY clause representation."""
    
    def __init__(self, column: str, direction: str = "ASC"):
        self.column = column
        self.direction = direction.upper()
    
    def __str__(self) -> str:
        return f"{self.column} {self.direction}"
    
    def to_sql(self) -> str:
        return str(self)

