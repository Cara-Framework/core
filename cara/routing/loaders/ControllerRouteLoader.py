"""
Controller Route Loader for loading routes from decorated controller methods.
"""

from __future__ import annotations

import inspect
from typing import Any

from cara.http.controllers import Controller
from cara.routing.Route import Route
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

    def load(self) -> list[Route]:
        """Load routes from controller methods."""
        collected: list[Route] = []

        for cls in self._all_controller_classes():
            for meta in self._get_decorated_methods(cls):
                instance = cls()
                handler = getattr(instance, meta["method_name"])

                name: str | None = meta.get("name")
                prefix: str | None = meta.get("prefix")
                namespace: str | None = meta.get("namespace")
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

    def _get_decorated_methods(self, controller_cls: Any) -> list[dict[str, Any]]:
        """Get methods decorated with @route from controller class."""
        found: list[dict[str, Any]] = []
        for name, method in inspect.getmembers(
            controller_cls, predicate=inspect.isfunction
        ):
            if hasattr(method, "__route__"):
                meta = dict(method.__route__)
                meta["method_name"] = name
                found.append(meta)
        return found

    def _all_controller_classes(self) -> list[Any]:
        """Get all controller classes from configured controllers module."""
        controllers_module_path = self.application.make("controllers.location")
        if not isinstance(controllers_module_path, str) or not controllers_module_path:
            raise RuntimeError("controllers.location must name an importable module")
        return get_classes(controllers_module_path, base_class=Controller)
