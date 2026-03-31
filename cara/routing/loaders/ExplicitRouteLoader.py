"""
Explicit Route Loader for loading routes from routes.api, routes.web, and routes.websocket modules.
"""

from typing import List

from cara.exceptions import RouteRegistrationException
from cara.facades import Log
from cara.routing import Route
from cara.support.ModuleLoader import load


class ExplicitRouteLoader:
    """Loads explicit routes from routes.api.register_routes(), routes.web.register_routes(), and routes.websocket.register_routes()"""

    def __init__(self, application):
        self.application = application

    def load(self) -> List[Route]:
        """Load explicit routes from all route modules."""
        all_routes = []

        # Define route modules to load
        route_modules = [
            ("routes.api.location", "routes.api"),
            ("routes.web.location", "routes.web"),
            ("routes.websocket.location", "routes.websocket"),
        ]

        # Load routes from each module
        for location_key, module_name in route_modules:
            routes = self._load_from_location(location_key, module_name)
            all_routes.extend(routes)

        return all_routes

    def _load_from_location(self, location_key: str, module_name: str) -> List[Route]:
        """Load routes from a specific location."""
        try:
            module = load(self.application.make(location_key))
            if hasattr(module, "register_routes"):
                raw = module.register_routes()

                if isinstance(raw, list):
                    return raw
                if raw is None:
                    return []
                return [raw]
            else:
                return []

        except (ImportError, AttributeError) as e:
            Log.warning(
                f"'{module_name}' not found OR error when importing this module: {e}"
            )
            return []
        except Exception as e:
            Log.error(
                f"Unexpected error during route registration from {module_name}: {e}"
            )
            raise RouteRegistrationException(
                f"Unexpected error during route registration from {module_name}: {e}"
            )
