"""
Controller Route Loader for loading routes from decorated controller methods.
"""

import inspect
from typing import Any, Dict, List, Optional

from cara.routing import Route
from cara.support import get_classes


class ControllerRouteLoader:
    """Loads routes from controller methods with @route decorators"""

    def __init__(self, application):
        self.application = application
        self._setup_controller_locations()

    def _setup_controller_locations(self) -> None:
        """Setup controller locations for Route factory."""
        controllers_location = self.application.make("controllers.location")
        Route.set_controller_locations(controllers_location)

    def load(self) -> List[Route]:
        """Load routes from controller methods."""
        collected: List[Route] = []

        for cls in self._all_controller_classes():
            for meta in self._get_decorated_methods(cls):
                instance = cls()
                handler = getattr(instance, meta["method_name"])

                name: Optional[str] = meta.get("name")
                prefix: Optional[str] = meta.get("prefix")
                namespace: Optional[str] = meta.get("namespace")
                middleware = meta.get("middleware")
                methods = meta["methods"]
                path = meta["path"]

                route_obj = Route.factory(
                    url=path,
                    controller=handler,
                    request_method=methods,
                    name=name,
                    prefix=prefix,
                    namespace=namespace,
                )

                if middleware:
                    route_obj.middleware(middleware)

                collected.append(route_obj)

        return collected

    def _get_decorated_methods(self, controller_cls: Any) -> List[Dict[str, Any]]:
        """Get methods decorated with @route from controller class."""
        found: List[Dict[str, Any]] = []
        for name, method in inspect.getmembers(
            controller_cls, predicate=inspect.isfunction
        ):
            if hasattr(method, "__route__"):
                meta = dict(method.__route__)
                meta["method_name"] = name
                found.append(meta)
        return found

    def _all_controller_classes(self) -> List[Any]:
        """Get all controller classes from configured controllers module."""
        try:
            # Get controllers module path from configuration
            controllers_module_path = self.application.make("controllers.location")

            # Use fluent helper to get controller classes - Clean & Simple!
            from cara.http.controllers import Controller

            # Direct module path usage with fluent helper
            controller_classes = get_classes(
                controllers_module_path, base_class=Controller
            )

            return controller_classes

        except Exception:
            # Fallback to default app.controllers path if configuration fails
            # Framework tries standard path but gracefully handles if not available
            try:
                from cara.http.controllers import Controller

                return get_classes("app.controllers", base_class=Controller)
            except Exception:
                # No controllers found - return empty list (framework agnostic)
                return []
