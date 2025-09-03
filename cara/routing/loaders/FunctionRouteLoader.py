"""
Function Route Loader for loading routes from decorated standalone functions.
"""

from typing import List

from cara.decorators.route import all_pending, clear
from cara.facades import Log
from cara.routing import Route


class FunctionRouteLoader:
    """Loads routes from standalone functions with @route decorators"""

    def __init__(self, application):
        self.application = application

    def load(self) -> List[Route]:
        """Load routes from decorated standalone functions."""
        collected: List[Route] = []

        for pending in all_pending():
            handler = pending["handler"]
            methods = pending["methods"]
            path = pending["path"]
            name = pending.get("name")
            prefix = pending.get("prefix")
            namespace = pending.get("namespace")
            middleware = pending.get("middleware")

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

        # Log duplicate route names
        self._check_duplicate_names(collected)

        return collected

    def clear(self) -> None:
        """Clear pending route decorators."""
        clear()

    def _check_duplicate_names(self, routes: List[Route]) -> None:
        """Check for duplicate route names and warn."""
        seen_names = set()
        for route in routes:
            name = route.get_name()
            if name:
                if name in seen_names:
                    Log.warning(
                        f"Duplicate route name detected: '{name}', overriding previous"
                    )
                seen_names.add(name)
