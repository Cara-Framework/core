"""
View Directives - Directive management for Cara view engine

This file provides directive registration and management functionality.
"""

from typing import Callable, Dict


class ViewDirectives:
    """Directive manager for view engine."""

    def __init__(self):
        """Initialize view directives."""
        self.directives = {}

    def register(self, name: str, handler: Callable):
        """Register a custom directive."""
        self.directives[name] = handler

    def has(self, name: str) -> bool:
        """Check if directive exists."""
        return name in self.directives

    def get(self, name: str) -> Callable:
        """Get directive handler."""
        return self.directives.get(name)

    def remove(self, name: str):
        """Remove a directive."""
        if name in self.directives:
            del self.directives[name]

    def all(self) -> Dict[str, Callable]:
        """Get all registered directives."""
        return self.directives.copy()

    def clear(self):
        """Clear all directives."""
        self.directives.clear()

    def extend(self, directives: Dict[str, Callable]):
        """Extend with multiple directives."""
        self.directives.update(directives)
