"""
View - Main view factory for Cara framework

This file provides the main View factory functionality.
"""

from typing import Any, Callable, Dict, List

from cara.view import ViewEngine, ViewInstance


class View:
    """Main View factory for creating and managing views."""

    def __init__(self, engine: ViewEngine = None):
        """Initialize view factory."""
        self.engine = engine or ViewEngine()
        self.shared_data = {}
        self.composers = {}
        self.creators = {}

    def make(self, view: str, data: Dict[str, Any] = None) -> ViewInstance:
        """Create a view instance."""
        return ViewInstance(view, data or {}, self.engine, self)

    def render(self, view: str, data: Dict[str, Any] = None) -> str:
        """Render a view template."""
        data = data or {}

        # Apply view composers
        data = self._apply_composers(view, data)

        return self.engine.render(view, data, factory=self)

    def render_string(self, template: str, data: Dict[str, Any] = None) -> str:
        """Render template string directly."""
        data = data or {}

        # Apply composers for string templates
        data = self._apply_composers("*", data)

        renderer = self.engine.get_renderer(factory=self)
        return renderer.render_string(template, data)

    def exists(self, view: str) -> bool:
        """Check if view template exists."""
        return self.engine.exists(view)

    def render_mail(self, view: str, data: Dict[str, Any] = None) -> str:
        """Render mail template with mail-specific data."""
        renderer = self.engine.get_renderer(factory=self)
        return renderer.render_mail_template(view, data)

    def render_mail_template(self, view: str, data: Dict[str, Any] = None) -> str:
        """Alias for render_mail method."""
        return self.render_mail(view, data)

    def share(self, key: str, value: Any) -> "View":
        """Share data globally across all views."""
        self.shared_data[key] = value
        return self

    def get_shared_data(self) -> Dict[str, Any]:
        """Get all shared data."""
        return self.shared_data.copy()

    def composer(self, pattern: str, callback: Callable) -> "View":
        """Register view composer for specific view patterns."""
        if pattern not in self.composers:
            self.composers[pattern] = []
        self.composers[pattern].append(callback)
        return self

    def get_composers(self, view: str) -> List[Callable]:
        """Get composers for a specific view."""
        composers = []
        for pattern, callbacks in self.composers.items():
            if self._pattern_matches(pattern, view):
                composers.extend(callbacks)
        return composers

    def creator(self, view: str, callback: Callable) -> "View":
        """Register view creator."""
        self.creators[view] = callback
        return self

    def directive(self, name: str, handler: Callable) -> "View":
        """Register custom directive."""
        self.engine.directive(name, handler)
        return self

    def get_engine(self) -> ViewEngine:
        """Get the view engine instance."""
        return self.engine

    def get_renderer(self, factory=None):
        """Get renderer instance with factory reference."""
        return self.engine.get_renderer(factory=factory or self)

    def clear_cache(self) -> "View":
        """Clear compiled template cache."""
        self.engine.clear_cache()
        return self

    def flush_state(self) -> "View":
        """Flush the factory state."""
        self.shared_data.clear()
        self.composers.clear()
        self.creators.clear()
        return self

    def flush_state_if_done_for_request(self) -> "View":
        """Flush state if done for current request."""
        return self.flush_state()

    def add_namespace(self, namespace: str, hints: List[str]) -> "View":
        """Add namespace for view location."""
        # TODO: Implementation for view namespaces
        return self

    def replace_namespace(self, namespace: str, hints: List[str]) -> "View":
        """Replace namespace hints."""
        # TODO: Implementation for replacing view namespaces
        return self

    # Private helper methods
    def _apply_composers(self, view: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply view composers to data."""
        composers = self.get_composers(view)

        for composer in composers:
            view_obj = self._create_view_object(data)
            composer(view_obj)
            data.update(view_obj.data_dict)

        return data

    def _create_view_object(self, data: Dict[str, Any]):
        """Create view object for composer."""

        class ViewObject:
            def __init__(self, data_dict):
                self.data_dict = data_dict
                self.data = data_dict

            def with_data(self, key, value=None):
                if isinstance(key, dict):
                    self.data_dict.update(key)
                    self.data.update(key)
                else:
                    self.data_dict[key] = value
                    self.data[key] = value

        return ViewObject(data)

    def _pattern_matches(self, pattern: str, view: str) -> bool:
        """Check if pattern matches view name."""
        return (
            pattern == "*" or pattern in view or view.startswith(pattern.replace("*", ""))
        )
