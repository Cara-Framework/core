"""
InsertBuilder - Single Responsibility for INSERT operations
"""
from typing import Any, Dict, List


class InsertBuilder:
    """Single responsibility: Build INSERT operations for queries."""
    
    def __init__(self):
        self._table = None
        self._values = []
        self._bindings = []
    
    def into(self, table: str):
        """Set the table to insert into."""
        self._table = table
        return self
    
    def values(self, data: Dict[str, Any]):
        """Add values to insert."""
        self._values.append(data)
        return self
    
    def get_values(self) -> List[Dict[str, Any]]:
        """Get all values."""
        return self._values.copy()
    
    def get_bindings(self) -> List[Any]:
        """Get all bindings."""
        return self._bindings.copy()
    
    def reset(self):
        """Reset all INSERT settings."""
        self._table = None
        self._values = []
        self._bindings = []
        return self

