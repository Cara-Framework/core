"""
Route Listing Command for the Cara framework.

This module provides a CLI command to list application routes with enhanced UX.
"""

import inspect
import re
from typing import Any, List, Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.routing import RouteResolver


@command(
    name="routes:list",
    help="List all application routes with basic filtering options.",
    options={
        "--filter=?": "Filter routes by URI or name pattern",
    },
)
class RouteListCommand(CommandBase):
    """List application routes with enhanced display."""

    def handle(self, filter: Optional[str] = None):
        """Handle route listing with basic filtering."""
        self.info("🛣️  Application Routes")

        # Get all routes
        try:
            routes = self._get_routes()
        except Exception as e:
            self.error(f"❌ Failed to retrieve routes: {e}")
            return

        # Apply filter if provided
        if filter:
            routes = self._filter_routes(routes, filter)
            if not routes:
                self.warning(f"⚠️  No routes found matching '{filter}'")
                return

        # Show routes
        self._show_routes(routes)

    def _get_routes(self) -> List[Any]:
        """Get all registered routes."""
        router = self.application.make("router")
        routes = list(router.routes)

        if not routes:
            raise RuntimeError("No routes are registered")

        return routes

    def _filter_routes(self, routes: List[Any], pattern: str) -> List[Any]:
        """Filter routes by pattern matching URI or name."""
        try:
            regex = re.compile(pattern, re.IGNORECASE)
            filtered = []

            for route in routes:
                # Check URI
                if regex.search(route.url):
                    filtered.append(route)
                    continue

                # Check name
                name = route.get_name()
                if name and regex.search(name):
                    filtered.append(route)

            self.info(f"🔍 Filtered by pattern: '{pattern}' ({len(filtered)} matches)")
            return filtered

        except re.error as e:
            self.error(f"❌ Invalid filter pattern '{pattern}': {e}")
            return []

    def _show_routes(self, routes: List[Any]) -> None:
        """Display routes in a table."""
        headers = ["URI", "Name", "Methods", "Controller", "Middleware"]
        rows = []

        for route in routes:
            rows.append(
                [
                    route.url,
                    route.get_name() or "—",
                    ", ".join(sorted(m.upper() for m in route.request_method)),
                    self._get_controller_name(route),
                    self._get_middleware_info(route),
                ]
            )

        self.info(f"📋 Found {len(routes)} route(s):")
        self.table(headers, rows)

        # Show basic stats
        self._show_stats(routes)

    def _get_controller_name(self, route: Any) -> str:
        """Get a simple controller name."""
        controller = route.controller

        # Handle RouteResolver
        if isinstance(controller, RouteResolver):
            real_controller = getattr(controller, "_route_handler", None) or getattr(
                controller, "handler", None
            )
            if real_controller:
                controller = real_controller
            else:
                return "Resolver"

        # Simple controller representation
        if isinstance(controller, str):
            return controller
        elif inspect.ismethod(controller):
            return f"{controller.__self__.__class__.__name__}@{controller.__name__}"
        elif inspect.isfunction(controller):
            return controller.__name__
        elif callable(controller):
            return controller.__class__.__name__
        else:
            return str(controller)

    def _get_middleware_info(self, route: Any) -> str:
        """Get middleware information for a route."""
        middlewares = []

        # Use the route's get_middleware() method if available
        if hasattr(route, "get_middleware"):
            try:
                route_middlewares = route.get_middleware()
                if route_middlewares:
                    middlewares.extend(route_middlewares)
            except Exception:
                pass

        # Fallback: Check for route-specific middleware attribute
        if not middlewares and hasattr(route, "middleware") and route.middleware:
            if isinstance(route.middleware, list):
                middlewares.extend(route.middleware)
            else:
                middlewares.append(route.middleware)

        # Check for middleware in route metadata
        if hasattr(route, "metadata") and route.metadata:
            route_middleware = route.metadata.get("middleware", [])
            if route_middleware:
                if isinstance(route_middleware, list):
                    middlewares.extend(route_middleware)
                else:
                    middlewares.append(route_middleware)

        # Format middleware names
        if middlewares:
            formatted = []
            for middleware in middlewares:
                if isinstance(middleware, str):
                    # Extract class name from middleware string
                    if ":" in middleware:
                        middleware = middleware.split(":")[0]
                    # Clean up common middleware names
                    clean_name = middleware.replace("Middleware", "").replace(
                        "middleware", ""
                    )
                    if clean_name:
                        formatted.append(clean_name)
                    else:
                        formatted.append(middleware)
                elif hasattr(middleware, "__name__"):
                    name = middleware.__name__.replace("Middleware", "")
                    formatted.append(name if name else middleware.__name__)
                elif hasattr(middleware, "__class__"):
                    name = middleware.__class__.__name__.replace("Middleware", "")
                    formatted.append(name if name else middleware.__class__.__name__)
                else:
                    formatted.append(str(middleware))

            return ", ".join(formatted[:3])  # Limit to first 3 to keep table readable

        return "—"

    def _show_stats(self, routes: List[Any]) -> None:
        """Show enhanced route statistics."""
        methods = set()
        named_count = 0
        middleware_count = 0
        controllers = set()
        prefixes = set()

        for route in routes:
            # HTTP methods
            methods.update(m.upper() for m in route.request_method)

            # Named routes
            if route.get_name():
                named_count += 1

            # Controllers
            controllers.add(self._get_controller_name(route))

            # Middleware
            middleware_info = self._get_middleware_info(route)
            if middleware_info != "—":
                middleware_count += 1

            # URL prefixes (extract first part of URL)
            url_parts = route.url.strip("/").split("/")
            if url_parts and url_parts[0]:
                prefixes.add(url_parts[0])

        # Create stats string with icons
        stats_parts = [
            f"🛣️  {len(routes)} routes",
            f"🌐 {len(methods)} HTTP methods",
            f"🏷️  {named_count} named routes",
            f"🛡️  {middleware_count} protected routes",
            f"🎛️  {len(controllers)} controllers",
        ]

        if prefixes:
            stats_parts.append(f"📁 {len(prefixes)} URL prefixes")

        self.info(f"\n📊 Statistics: {' • '.join(stats_parts)}")

        # Show available methods
        if methods:
            self.info(f"🔗 HTTP Methods: {', '.join(sorted(methods))}")

        # Show prefixes if any
        if prefixes and len(prefixes) <= 10:  # Don't show if too many
            sorted_prefixes = sorted(p for p in prefixes if p)
            if sorted_prefixes:
                self.info(f"📂 URL Prefixes: {', '.join(sorted_prefixes)}")
