"""
JoinClause - Simple JOIN clause component (different from expression)
"""

class JoinClause:
    """Simple JOIN clause component representation."""
    
    def __init__(self, join_type: str, table: str, on_conditions: list = None):
        self.join_type = join_type
        self.table = table
        self.on_conditions = on_conditions or []
    
    def add_on_condition(self, condition):
        """Add an ON condition."""
        self.on_conditions.append(condition)
        return self
    
    def __str__(self) -> str:
        return f"{self.join_type} JOIN {self.table}"
    
    def to_sql(self) -> str:
        return str(self)

