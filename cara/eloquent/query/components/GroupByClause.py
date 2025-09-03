"""
GroupByClause - Simple GROUP BY clause component
"""

class GroupByClause:
    """Simple GROUP BY clause representation."""
    
    def __init__(self, *columns):
        self.columns = list(columns)
    
    def add_column(self, column: str):
        """Add a column to GROUP BY."""
        self.columns.append(column)
        return self
    
    def __str__(self) -> str:
        return ", ".join(self.columns)
    
    def to_sql(self) -> str:
        return str(self)

