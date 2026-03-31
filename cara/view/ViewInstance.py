"""
ViewInstance - Individual view instance for Cara framework

This file provides individual view instance functionality.
"""

from typing import Any, Dict


class ViewInstance:
    """Individual view instance."""

    def __init__(self, view: str, data: Dict[str, Any], engine, factory):
        """Initialize view instance."""
        self.view = view
        self.data = data
        self.engine = engine
        self.factory = factory

    def with_data(self, key: str, value: Any = None) -> "ViewInstance":
        """Add data to view."""
        if isinstance(key, dict):
            self.data.update(key)
        else:
            self.data[key] = value
        return self

    def with_shared_data(self) -> "ViewInstance":
        """Add shared data to view."""
        shared_data = self.factory.get_shared_data()
        self.data = {**shared_data, **self.data}
        return self

    def render(self) -> str:
        """Render the view."""
        # Apply shared data
        self.with_shared_data()

        # Apply view creators
        if self.view in self.factory.creators:
            creator = self.factory.creators[self.view]
            creator(self)

        # Apply view composers
        composers = self.factory.get_composers(self.view)
        for composer in composers:
            composer(self)

        # Render using engine with factory
        return self.engine.render(self.view, self.data, factory=self.factory)

    def __str__(self) -> str:
        """String representation renders the view."""
        return self.render()
