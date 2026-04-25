"""
Middleware capsule for managing middleware execution.
Laravel-style middleware management with parameter parsing support.
"""

from typing import Dict, Iterator, List, Optional, Set, Type, Union

from cara.exceptions import RouteMiddlewareNotFoundException
from cara.middleware import Middleware

MiddlewareType = Type[Middleware]


class MiddlewareCapsule:
    """Middleware Capsule for managing middleware execution."""

    def __init__(self, application):
        self.application = application
        self._global_middleware: List[MiddlewareType] = []
        self._route_middleware: Dict[str, List[MiddlewareType]] = {}
        self._middleware_aliases: Dict[str, MiddlewareType] = {}
        self._terminable_middleware: Set[MiddlewareType] = set()
        # Laravel-style middleware priority ordering. Middleware classes in
        # this list will be sorted into the order specified regardless of
        # how they were registered. Unknown middleware keep registration
        # order and appear after all prioritized ones.
        self._priority: List[MiddlewareType] = []

    def __iter__(self) -> Iterator[MiddlewareType]:
        return iter(self._global_middleware)

    def __len__(self) -> int:
        return len(self._global_middleware)

    def __contains__(self, item: MiddlewareType) -> bool:
        return item in self._global_middleware

    def __getitem__(self, key: str) -> List[MiddlewareType]:
        if key in self._route_middleware:
            return self._route_middleware[key]
        raise RouteMiddlewareNotFoundException(f"Middleware group '{key}' not found")

    def add_global(self, middleware: MiddlewareType) -> "MiddlewareCapsule":
        """Add global middleware."""
        if middleware not in self._global_middleware:
            self._global_middleware.append(middleware)
        return self

    def add(self, middleware: MiddlewareType) -> "MiddlewareCapsule":
        """Add global middleware (alias for add_global)."""
        return self.add_global(middleware)

    def create_group(self, name: str) -> "MiddlewareCapsule":
        """Create a new middleware group."""
        if name not in self._route_middleware:
            self._route_middleware[name] = []
        return self

    def add_to_group(self, group: str, middleware: MiddlewareType) -> "MiddlewareCapsule":
        """Add middleware to a specific group."""
        if group not in self._route_middleware:
            self.create_group(group)

        if middleware not in self._route_middleware[group]:
            self._route_middleware[group].append(middleware)
        return self

    def add_route_middleware(self, group: str, mw: MiddlewareType) -> "MiddlewareCapsule":
        """Add route middleware (alias for add_to_group)."""
        return self.add_to_group(group, mw)

    def add_alias(self, name: str, middleware: MiddlewareType) -> "MiddlewareCapsule":
        """Add middleware alias for easier reference in routes."""
        self._middleware_aliases[name] = middleware
        return self

    def register_terminable(self, middleware: MiddlewareType) -> "MiddlewareCapsule":
        """Register middleware as terminable (runs after response is sent)."""
        self._terminable_middleware.add(middleware)
        return self

    def is_terminable(self, middleware: MiddlewareType) -> bool:
        """Check if middleware is registered as terminable.

        Routes that use parameterized middleware (e.g. ``throttle:60,1``,
        ``auth:jwt``) get a *fresh* ``ParameterizedMiddleware`` proxy class
        per resolution — see ``_create_parameterized_middleware``. The
        proxy class is never identity-equal to the base class registered
        in ``_terminable_middleware``, so a direct ``in`` check failed and
        ``terminate()`` never ran for parameterized middleware. Unwrap
        through ``__base_middleware__`` so the lookup matches the
        registration.
        """
        if middleware in self._terminable_middleware:
            return True
        base = getattr(middleware, "__base_middleware__", None)
        if base is not None and base in self._terminable_middleware:
            return True
        return False

    def get_terminable_middleware(self) -> Set[MiddlewareType]:
        """Get all registered terminable middleware."""
        return self._terminable_middleware

    def resolve_alias(self, name: str) -> Optional[MiddlewareType]:
        """Resolve middleware alias to actual middleware class."""
        return self._middleware_aliases.get(name)

    def resolve_middleware(
        self, middleware: Union[str, MiddlewareType]
    ) -> Union[MiddlewareType, List[MiddlewareType], None]:
        """
        Resolve middleware from string alias, group name, or class.
        Supports Laravel-style parameters: 'auth:jwt', 'throttle:60,1'
        """
        if isinstance(middleware, str):
            # Check for Laravel-style parameters
            if ":" in middleware:
                middleware_name, params = middleware.split(":", 1)
                return self._resolve_middleware_with_params(middleware_name, params)

            # Try alias lookup first
            aliased = self.resolve_alias(middleware)
            if aliased:
                return aliased

            # Try group lookup
            group_list = self._route_middleware.get(middleware)
            if group_list is not None:
                return group_list.copy()

            return None

        return middleware

    def _resolve_middleware_with_params(
        self, middleware_name: str, params: str
    ) -> Optional[MiddlewareType]:
        """Resolve middleware with Laravel-style parameters."""
        # Get the base middleware class
        base_middleware = self.resolve_alias(middleware_name)
        if not base_middleware:
            # Try group lookup as fallback
            group_list = self._route_middleware.get(middleware_name)
            if group_list and len(group_list) == 1:
                base_middleware = group_list[0]
            else:
                return None

        # Parse parameters
        param_list = [param.strip() for param in params.split(",")]

        # Create parameterized middleware class
        return self._create_parameterized_middleware(base_middleware, param_list)

    def _create_parameterized_middleware(
        self, base_middleware: MiddlewareType, parameters: List[str]
    ) -> MiddlewareType:
        """Create a parameterized middleware class using Laravel-style parameter parsing."""

        class ParameterizedMiddleware:
            def __init__(self, application):
                self._instance = base_middleware.create_with_parameters(
                    application, parameters
                )

            def __getattr__(self, name):
                return getattr(self._instance, name)

            async def handle(self, request, next_fn):
                return await self._instance.handle(request, next_fn)

            async def terminate(self, request, response):
                if hasattr(self._instance, "terminate"):
                    return await self._instance.terminate(request, response)

        # Set meaningful name for debugging
        ParameterizedMiddleware.__name__ = f"{base_middleware.__name__}WithParams"
        ParameterizedMiddleware.__qualname__ = f"{base_middleware.__qualname__}WithParams"
        # Expose base class so priority-ordering can unwrap the proxy
        ParameterizedMiddleware.__base_middleware__ = base_middleware
        return ParameterizedMiddleware

    def remove(self, mw: Union[str, MiddlewareType]) -> "MiddlewareCapsule":
        """Remove middleware from global list or clear a group."""
        if isinstance(mw, str):
            if mw in self._route_middleware:
                self._route_middleware[mw] = []
        else:
            try:
                self._global_middleware.remove(mw)
            except ValueError:
                pass
            if mw in self._terminable_middleware:
                self._terminable_middleware.remove(mw)
        return self

    def get_global_middleware(self) -> List[MiddlewareType]:
        """Get global middleware sorted by priority."""
        return self.sort_by_priority(self._global_middleware.copy())

    def set_priority(self, priority: List[MiddlewareType]) -> "MiddlewareCapsule":
        """Set the middleware priority order (Laravel ``$middlewarePriority``).

        Middleware classes present in ``priority`` are always ordered as
        specified; classes not in the list keep their relative registration
        order and appear after prioritized ones.
        """
        self._priority = list(priority)
        return self

    def get_priority(self) -> List[MiddlewareType]:
        """Return the configured priority list."""
        return list(self._priority)

    def sort_by_priority(self, middleware: List[MiddlewareType]) -> List[MiddlewareType]:
        """Sort a middleware stack according to the configured priority.

        Classes present in ``_priority`` are emitted in that order, followed
        by any remaining middleware in their original registration order.
        """
        if not self._priority:
            return middleware

        priority_index = {cls: i for i, cls in enumerate(self._priority)}

        def _base(mw: MiddlewareType) -> MiddlewareType:
            """Unwrap a ParameterizedMiddleware back to its base class."""
            return getattr(mw, "__base_middleware__", mw)

        prioritized: List[MiddlewareType] = []
        remainder: List[MiddlewareType] = []
        for mw in middleware:
            if _base(mw) in priority_index:
                prioritized.append(mw)
            else:
                remainder.append(mw)
        prioritized.sort(key=lambda m: priority_index[_base(m)])
        return prioritized + remainder

    def get_route_middleware(self, group: str) -> List[MiddlewareType]:
        """Get middleware for a specific route group."""
        if group in self._route_middleware:
            return self._route_middleware[group].copy()
        raise RouteMiddlewareNotFoundException(f"Middleware group '{group}' not found")

    def get_groups(self) -> List[str]:
        """Get all available middleware group names."""
        return list(self._route_middleware.keys())

    def get_aliases(self) -> Dict[str, MiddlewareType]:
        """Get all registered middleware aliases."""
        return self._middleware_aliases.copy()

    def load_from_registry(self, registry_config: Dict) -> "MiddlewareCapsule":
        """Load middleware configuration from MiddlewareRegistry build output."""
        # Load global middleware
        for middleware in registry_config.get("global", []):
            self.add_global(middleware)

        # Load groups
        for group_name, middlewares in registry_config.get("groups", {}).items():
            self.create_group(group_name)
            for middleware in middlewares:
                self.add_to_group(group_name, middleware)

        # Load aliases
        for alias_name, middleware in registry_config.get("aliases", {}).items():
            self.add_alias(alias_name, middleware)

        # Load priority ordering (if provided)
        priority = registry_config.get("priority")
        if priority:
            self.set_priority(priority)

        return self
