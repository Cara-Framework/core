"""
Deferred provider that loads middleware configuration from MiddlewareRegistry.
Binds the configured MiddlewareCapsule under the key 'middleware_http' and 'middleware_ws'.
"""

import inspect
from typing import Dict, List, Type

from cara.configuration import config
from cara.facades import Log
from cara.foundation import DeferredProvider
from cara.middleware import Middleware, MiddlewareCapsule
from cara.middleware.http import (
    AttachRequestID,
    CanPerform,
    CheckMaintenanceMode,
    ShouldAuthenticate,
    ThrottleRequests,
    TrimStrings,
)
from cara.middleware.ws import Authenticate, LogWSRequests

MiddlewareClass = Type[Middleware]


class MiddlewareProvider(DeferredProvider):
    @classmethod
    def provides(cls) -> List[str]:
        return ["middleware_http", "middleware_ws"]

    @property
    def default_http_middleware(self) -> List[MiddlewareClass]:
        """
        Core HTTP middleware that's always available.
        These are framework-level middleware, not user middleware.
        """
        return [
            AttachRequestID,
            CheckMaintenanceMode,
            ThrottleRequests,
            TrimStrings,
        ]

    @property
    def default_ws_middleware(self) -> List[MiddlewareClass]:
        """
        Core WebSocket middleware that's always available.
        """
        return [
            LogWSRequests,
            Authenticate,
        ]

    @staticmethod
    def validate_middleware_interface(middleware_class: MiddlewareClass):
        """
        Validate that middleware class implements the correct interface.

        Raises:
            ValueError: If middleware doesn't implement required methods
        """
        try:
            if not issubclass(middleware_class, Middleware):
                raise ValueError(
                    f"Middleware {middleware_class.__name__} must inherit from Middleware base class"
                )
        except TypeError:
            # Handle cases where middleware_class is not a class
            raise ValueError(f"Invalid middleware: {middleware_class} is not a class")

        # Check if handle method exists and has correct signature
        if not hasattr(middleware_class, "handle"):
            raise ValueError(
                f"Middleware {middleware_class.__name__} must implement 'handle(request, next)' method"
            )

        handle_method = getattr(middleware_class, "handle")
        if not callable(handle_method):
            raise ValueError(
                f"Middleware {middleware_class.__name__}.handle must be callable"
            )

        # Check method signature
        sig = inspect.signature(handle_method)
        params = list(sig.parameters.keys())

        # Expected: self, request, next (3 parameters)
        if len(params) < 3:
            raise ValueError(
                f"Middleware {middleware_class.__name__}.handle must accept (self, request, next) parameters. "
                f"Found parameters: {params}"
            )

    @staticmethod
    def validate_all_middleware(config_dict: Dict) -> None:
        """
        Validate all middleware classes in configuration.

        Args:
            config_dict: Configuration dictionary from MiddlewareRegistry
        """
        all_middleware = []

        # Collect all middleware from different sources
        all_middleware.extend(config_dict.get("global", []))

        for group_middleware in config_dict.get("groups", {}).values():
            all_middleware.extend(group_middleware)

        all_middleware.extend(config_dict.get("aliases", {}).values())

        # Validate each middleware
        for middleware_class in all_middleware:
            if middleware_class:  # Skip None values
                MiddlewareProvider.validate_middleware_interface(middleware_class)
            else:
                Log.error(f"Middleware {middleware_class} is None")

    @staticmethod
    def build_capsule_from_config(
        application,
        config_key: str,
        default_middleware: List[MiddlewareClass],
        bind_name: str,
    ) -> None:
        """
        Build middleware capsule from MiddlewareRegistry configuration.

        Args:
            application: The application instance
            config_key: Configuration key to read from
            default_middleware: Framework default middleware
            bind_name: Name to bind capsule under
        """
        # Create capsule
        capsule = MiddlewareCapsule(application)

        # Add core middleware aliases (available in all apps)
        MiddlewareProvider._register_core_aliases(capsule)

        # Get user configuration
        user_config = config(config_key, default={})

        # Validate configuration if present
        if user_config:
            MiddlewareProvider.validate_all_middleware(user_config)
            # Load user configuration into capsule
            capsule.load_from_registry(user_config)

        # Validate and add framework default middleware
        for middleware_class in default_middleware:
            MiddlewareProvider.validate_middleware_interface(middleware_class)
            capsule.add_global(middleware_class)

        # Bind to application
        application.bind(bind_name, capsule)

    @staticmethod
    def _register_core_aliases(capsule) -> None:
        """
        Register core framework middleware aliases.
        These are available in all Cara applications by default.
        """
        # Core HTTP middleware aliases
        capsule.add_alias("auth", ShouldAuthenticate)
        capsule.add_alias("can", CanPerform)
        capsule.add_alias("throttle", ThrottleRequests)

    def register(self) -> None:
        """Register HTTP and WebSocket middleware."""
        self.register_http_middleware()
        self.register_ws_middleware()

    def register_http_middleware(self):
        """Register HTTP middleware capsule."""
        self.build_capsule_from_config(
            self.application,
            config_key="middleware.http",
            default_middleware=self.default_http_middleware,
            bind_name="middleware_http",
        )

    def register_ws_middleware(self):
        """Register WebSocket middleware capsule."""
        self.build_capsule_from_config(
            self.application,
            config_key="middleware.websocket",
            default_middleware=self.default_ws_middleware,
            bind_name="middleware_ws",
        )
