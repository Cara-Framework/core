"""
View Renderer - Rendering functionality for Cara view engine

This file provides view rendering capabilities.
"""

from typing import Any, Dict

from cara.view import ViewEngine


class ViewRenderer:
    """View renderer for processing and rendering templates."""

    def __init__(self, engine: ViewEngine = None, factory=None):
        """Initialize view renderer."""
        self.engine = engine or ViewEngine()
        self.factory = factory
        self.debug = True  # Enable debug mode by default

    def render(self, view: str, data: Dict[str, Any] = None) -> str:
        """Render a view template."""
        return self.engine.render(view, data, factory=self.factory)

    def render_string(self, template: str, data: Dict[str, Any] = None) -> str:
        """Render template string directly."""
        data = data or {}

        # Merge shared data from factory if available
        if self.factory:
            shared_data = self.factory.get_shared_data()
            # Shared data has lower priority than local data
            merged_data = {**shared_data, **data}
        else:
            merged_data = data

        try:
            # Compile template
            compiled_code = self.engine.compiler.compile(template, self.engine.directives)

            # Create execution context
            context = {
                "__builtins__": __builtins__,
                "data": merged_data,
                "escape": self.engine.escape_html,
                "raw": lambda x: x,
            }

            # Apply factory logic if available (similar to ViewEngine.render)
            if self.factory and hasattr(self.factory, "get_composers"):
                composers = self.factory.get_composers(
                    "*"
                )  # Use wildcard for string templates
                for composer in composers:

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

                    view_obj = ViewObject(merged_data)
                    composer(view_obj)
                    merged_data.update(view_obj.data_dict)
                    context["data"] = merged_data

            # Add data variables directly to context with nested access support
            class DataWrapper:
                def __init__(self, data_dict):
                    self._data = data_dict

                def __getattr__(self, name):
                    if name in self._data:
                        value = self._data[name]
                        if isinstance(value, dict):
                            return DataWrapper(value)
                        return value
                    raise AttributeError(
                        f"'DataWrapper' object has no attribute '{name}'"
                    )

                def __getitem__(self, key):
                    return self._data[key]

                def __contains__(self, key):
                    return key in self._data

                def __str__(self):
                    return str(self._data)

                def __repr__(self):
                    return repr(self._data)

            # Add DataWrapper to context
            context["DataWrapper"] = DataWrapper

            # Add all data keys to context
            for key, value in merged_data.items():
                if isinstance(value, dict):
                    context[key] = DataWrapper(value)
                else:
                    context[key] = value

            # Execute compiled template
            exec(compiled_code, context)

            return context.get("__output__", "")

        except Exception as e:
            if self.debug:
                # Re-raise the original exception in debug mode
                raise e
            else:
                # Return error message in production mode
                return "Template Error"

    def render_partial(self, view: str, data: Dict[str, Any] = None) -> str:
        """Render a partial view (for includes)."""
        return self.render(view, data)

    def render_component(self, component: str, data: Dict[str, Any] = None) -> str:
        """Render a view component."""
        # Components would be views with specific naming convention
        component_view = f"components.{component}"
        return self.render(component_view, data)

    def render_mail_template(self, template: str, data: Dict[str, Any] = None) -> str:
        """Render mail template specifically."""
        # Add mail-specific data and helpers
        mail_data = {
            "app_name": "Cara Application",
            "app_url": "http://localhost",
        }

        # Merge with provided data
        if data:
            mail_data.update(data)

        return self.render(template, mail_data)

    def render_notification_template(
        self, template: str, data: Dict[str, Any] = None
    ) -> str:
        """Render notification template."""
        # Add notification-specific data
        notification_data = {
            "app_name": "Cara Application",
        }

        # Merge with provided data
        if data:
            notification_data.update(data)

        return self.render(template, notification_data)

    def exists(self, view: str) -> bool:
        """Check if view template exists."""
        return self.engine.exists(view)

    def get_engine(self) -> ViewEngine:
        """Get the view engine instance."""
        return self.engine

    def get_renderer(self) -> "ViewRenderer":
        """Get the renderer instance (for compatibility)."""
        return self
