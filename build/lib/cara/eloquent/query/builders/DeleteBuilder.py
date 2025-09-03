"""
DeleteBuilder - Single Responsibility for DELETE operations

Handles all DELETE-related query building operations cleanly and efficiently.
Follows DRY and KISS principles.
"""
from typing import Any, List, Optional


class DeleteBuilder:
    """
    Single responsibility: Build DELETE operations for queries.
    
    This builder handles:
    - Simple DELETE operations
    - Conditional DELETE operations
    - Bulk DELETE operations
    - Soft DELETE operations
    """
    
    def __init__(self):
        self._delete_table = None
        self._conditions = []
        self._bindings = []
        self._soft_delete = False
        self._deleted_at_column = "deleted_at"
    
    def delete_from(self, table: str) -> "DeleteBuilder":
        """Set the table to delete from."""
        self._delete_table = table
        return self
    
    def where(self, column: str, operator: str = "=", value: Any = None) -> "DeleteBuilder":
        """Add WHERE condition for DELETE."""
        if value is None:
            value = operator
            operator = "="
        
        self._conditions.append({
            "column": column,
            "operator": operator,
            "value": value,
            "boolean": "AND"
        })
        self._bindings.append(value)
        return self
    
    def get_conditions(self) -> List[dict]:
        """Get all WHERE conditions."""
        return self._conditions.copy()
    
    def get_bindings(self) -> List[Any]:
        """Get all bindings."""
        return self._bindings.copy()
    
    def has_conditions(self) -> bool:
        """Check if there are any conditions."""
        return len(self._conditions) > 0
    
    def reset(self) -> "DeleteBuilder":
        """Reset all DELETE settings."""
        self._delete_table = None
        self._conditions = []
        self._bindings = []
        self._soft_delete = False
        self._deleted_at_column = "deleted_at"
        return self
