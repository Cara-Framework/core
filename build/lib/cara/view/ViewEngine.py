"""
View Engine - Core view engine for Cara framework

This file provides the main view engine functionality.
"""

import os
from typing import Any, Dict, List, Optional

from cara.view import ViewCompiler, ViewDirectives, ViewDirectivesRegistry


class ViewEngine:
    """Core view engine for template processing."""

    def __init__(self, view_paths: List[str] = None, cache_path: str = None):
        """Initialize view engine."""
        from cara.support import paths

        self.view_paths = view_paths or [paths("views")]
        self.cache_path = cache_path or paths("cache")
        self.compiler = ViewCompiler()
        self.directives = ViewDirectives()
        self.compiled_cache = {}

        self._setup()

    def directive(self, name: str, handler):
        """Register custom directive."""
        self.directives.register(name, handler)

    def render(self, view: str, data: Dict[str, Any] = None, factory=None) -> str:
        """Render a view template."""
        data = data or {}

        # Merge with shared data if factory is available
        if factory:
            data = {**factory.get_shared_data(), **data}
            data = self._apply_factory_composers(view, data, factory)

        # Find and compile template
        template_path = self.find_view(view)
        if not template_path:
            raise FileNotFoundError(f"View '{view}' not found")

        compiled_code = self.get_compiled_template(template_path)

        # Create and execute template context
        context = self._create_execution_context(data)
        exec(compiled_code, context)

        return context.get("__output__", "")

    def find_view(self, view: str) -> Optional[str]:
        """Find view template file."""
        view_path = view.replace(".", os.sep)
        extensions = [".cara.html", ".html", ".htm"]

        for base_path in self.view_paths:
            for ext in extensions:
                full_path = os.path.join(base_path, f"{view_path}{ext}")
                if os.path.exists(full_path):
                    return full_path

        return None

    def exists(self, view: str) -> bool:
        """Check if view exists."""
        return self.find_view(view) is not None

    def get_compiled_template(self, template_path: str) -> str:
        """Get compiled template code."""
        if template_path in self.compiled_cache:
            return self.compiled_cache[template_path]

        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        compiled_code = self.compiler.compile(template_content, self.directives)
        self.compiled_cache[template_path] = compiled_code

        return compiled_code

    def clear_cache(self):
        """Clear compiled template cache."""
        self.compiled_cache.clear()

        if os.path.exists(self.cache_path):
            for file in os.listdir(self.cache_path):
                file_path = os.path.join(self.cache_path, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)

    def escape_html(self, value: Any) -> str:
        """Escape HTML characters."""
        if value is None:
            return ""

        value = str(value)

        # Check if already escaped
        if "&lt;" in value or "&gt;" in value or "&amp;" in value:
            return value

        return (
            value.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;")
        )

    def get_renderer(self, factory=None):
        """Get renderer instance."""
        from cara.view.ViewRenderer import ViewRenderer

        return ViewRenderer(self, factory)

    # Private methods
    def _setup(self):
        """Setup the view engine."""
        self._ensure_cache_directory()
        self._register_default_directives()

    def _ensure_cache_directory(self):
        """Ensure cache directory exists."""
        os.makedirs(self.cache_path, exist_ok=True)

    def _register_default_directives(self):
        """Register default template directives."""
        ViewDirectivesRegistry.register_defaults(self.directives)

    def _apply_factory_composers(
        self, view: str, data: Dict[str, Any], factory
    ) -> Dict[str, Any]:
        """Apply factory composers to data."""
        if not hasattr(factory, "get_composers"):
            return data

        composers = factory.get_composers(view)
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

    def _create_execution_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create template execution context."""

        class DataWrapper:
            def __init__(self, data_dict):
                self._data = data_dict

            def __getattr__(self, name):
                if name in self._data:
                    value = self._data[name]
                    if isinstance(value, dict):
                        return DataWrapper(value)
                    return value
                raise AttributeError(f"'DataWrapper' object has no attribute '{name}'")

            def __getitem__(self, key):
                return self._data[key]

            def __contains__(self, key):
                return key in self._data

            def __str__(self):
                return str(self._data)

            def __repr__(self):
                return repr(self._data)

        context = {
            "__builtins__": __builtins__,
            "data": data,
            "escape": self.escape_html,
            "raw": lambda x: x,
            "DataWrapper": DataWrapper,
        }

        # Add all data keys to context
        for key, value in data.items():
            if isinstance(value, dict):
                context[key] = DataWrapper(value)
            else:
                context[key] = value

        return context
