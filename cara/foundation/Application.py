"""
Core Application Module.

This module contains the Application class which serves as the central hub of the Cara framework. It
manages the container, service providers, and implements the ASGI interface for handling HTTP
requests and lifespan events. The Application class is responsible for bootstrapping the framework
and coordinating all its components.
"""

import inspect
import threading
from typing import Any, Dict, List, Optional, Type

from cara.container import Container
from cara.environment import LoadEnvironment

# Lazy import to avoid circular imports
from cara.foundation import DeferredProvider, Provider
from cara.support.PathManager import PathManager


class Application(Container):
    """
    Core Application class.

    Extends the Container so that:
      - Non‐deferred providers register immediately.
      - Deferred providers (those subclassing DeferredProvider) are stored in
        deferred_providers and only registered when you first request them,
        whether by string key or by type hint.

    Implements the ASGI interface and exposes `make(...)` for dependency resolution.
    """

    def __init__(self, base_path: Optional[str] = None):
        """Initialize the application (and its underlying Container)."""
        super().__init__()  # Initialize Container internals

        # Load environment variables early, before any providers
        LoadEnvironment()

        self.base_path = base_path
        self.storage_path = None

        # Set the base path in PathManager so paths() function works correctly
        if base_path:
            PathManager.set_base_path(base_path)

        # All providers that have been eagerly instantiated & registered
        self.providers: List[Provider] = []
        # Lock around deferred provider resolution.
        #
        # ROOT CAUSE (2026-04-23): ``Application.make()`` has the same
        # deferred-provider race that Container.make() was hit by —
        # two worker threads call ``make("cache")`` simultaneously,
        # the first pops the entry out of ``deferred_providers`` and
        # enters ``provider.register()``, the second arrives in the
        # narrow window after A's pop but before A's ``bind("cache",
        # ...)`` completes, sees the key absent from both the deferred
        # dict (A popped it) and the container ``objects`` (A hasn't
        # bound yet), and raises ``MissingContainerBindingException``
        # from ``super().make(name)`` at the bottom. Fixing Container
        # alone was not enough — Application owns its own separate
        # ``deferred_providers`` dict and has its own critical
        # section. Same RLock pattern.
        self._deferred_providers_lock = threading.RLock()
        # Map of deferred binding key (string) → provider class
        self.deferred_providers: Dict[str, Type[DeferredProvider]] = {}

    def register_providers(self, *providers: Type[Any]) -> "Application":
        """
        Register framework‐internal service providers.

        - If provider_class is a DeferredProvider, store its provides() keys
          in deferred_providers (do NOT instantiate/register now).
        - Otherwise, instantiate and call register() immediately.
        """
        for provider_class in providers:
            if issubclass(provider_class, DeferredProvider):
                # For each key this provider "provides", defer registration
                for binding_key in provider_class.provides():
                    self.deferred_providers[binding_key] = provider_class
            else:
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)

        return self

    def add_providers(self, *providers: Type[Any]) -> "Application":
        """
        Register application‐specific providers.

        Same logic as register_providers: deferred go into deferred_providers,
        others register immediately.
        """
        for provider_class in providers:
            if issubclass(provider_class, DeferredProvider):
                for binding_key in provider_class.provides():
                    self.deferred_providers[binding_key] = provider_class
            else:
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)

        return self

    def make(self, name: Any, *arguments: Any) -> Any:
        """
        Override Container.make(...) so that:

          - If `name` is a string and is deferred, register+boot that provider now.
          - If `name` is a class and its lowercase name appears in deferred_providers,
            register+boot that provider now (allows type‐hint injection).
          - Then delegate to super().make(name, ...).

        This way, deferred providers fire when you first request them by key or by class.

        ROOT CAUSE (2026-04-24): the previous implementation did a
        lock-free outer check (``if name in self.deferred_providers``)
        and only acquired the RLock when it saw the key. That leaves a
        window where thread A has already popped the key and is inside
        ``register()`` (building the Cache manager, adding drivers),
        but thread B's outer check sees the key absent, SKIPS the
        locked block entirely, and falls straight through to
        ``super().make(name)``. Container's ``objects`` doesn't have
        the key yet (A hasn't called ``bind`` yet) and ``_deferred`` is
        empty — Container uses its own ``_deferred`` dict that
        Application bypasses — so B raises
        ``MissingContainerBindingException: 'cache' key was not found``.
        Under ``--concurrency=8`` this was the "Facade 'cache' could
        not resolve 'get'" spray that DLQ'd a batch of CollectProductJobs.

        Fix: check for the deferred key AND call ``super().make()``
        inside a single critical section. Now B is forced to wait
        behind A's lock, and by the time B gets to ``super().make()``
        the binding is live.
        """
        with self._deferred_providers_lock:
            # 1) If caller is requesting by class
            if inspect.isclass(name):
                key_str = name.__name__.lower()
                if key_str in self.deferred_providers:
                    provider_class = self.deferred_providers.pop(key_str)
                    for k, cls in list(self.deferred_providers.items()):
                        if cls is provider_class:
                            self.deferred_providers.pop(k, None)
                    provider = provider_class(self)
                    provider.register()
                    self.providers.append(provider)
                    if hasattr(provider, "boot"):
                        provider.boot()
                # Now that (maybe) registered, delegate to Container.make
                return super().make(name, *arguments)

            # 2) If caller is requesting by string
            if isinstance(name, str) and name in self.deferred_providers:
                provider_class = self.deferred_providers.pop(name)
                for k, cls in list(self.deferred_providers.items()):
                    if cls is provider_class:
                        self.deferred_providers.pop(k, None)
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)
                if hasattr(provider, "boot"):
                    provider.boot()

            # 3) Delegate to base Container.make
            return super().make(name, *arguments)

    def has(self, name: Any) -> bool:
        """
        Check if a binding exists (including deferred providers).

        Override Container.has() to also check deferred_providers.
        """
        # Check regular bindings first
        if super().has(name):
            return True

        # Check deferred providers
        if isinstance(name, str) and name in self.deferred_providers:
            return True

        return False

    def boot_providers(self) -> None:
        """
        Call boot() on all already‐registered (non‐deferred) providers.
        Also initialize router to catch route registration errors early.
        """
        for provider in self.providers:
            if hasattr(provider, "boot"):
                provider.boot()

        # Initialize router early to catch route registration errors
        try:
            self.router = self.make("router")
        except Exception as e:
            # Check if it's a RouteRegistrationException
            from cara.exceptions import RouteRegistrationException

            if isinstance(e, RouteRegistrationException):
                raise RuntimeError(
                    f"Application startup failed due to route configuration: {e}"
                ) from e
            else:
                raise RuntimeError(f"Application startup failed: {e}") from e

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI application interface: delegate to HTTP or lifespan conductor."""
        self.initialize_conductors()
        self.add_app_to_scope(scope)
        await self.handle_request(scope, receive, send)

    def initialize_conductors(self):
        """Ensure http_conductor and lifespan_conductor are resolved (may fire deferred)."""
        # Request by string "http_conductor" → may register that deferred provider now
        self.http_conductor = self.make("http_conductor")
        if not self.http_conductor:
            raise Exception("HTTP conductor must be registered.")

        # Request by string "lifespan_conductor" → may register that deferred provider now
        self.lifespan_conductor = self.make("lifespan_conductor")
        if not self.lifespan_conductor:
            raise Exception("Lifespan conductor must be registered.")

        self.websocket_conductor = self.make("websocket_conductor")
        if not self.websocket_conductor:
            raise Exception("Websocket conductor must be registered.")

    def add_app_to_scope(self, scope: dict) -> None:
        """Attach application instance into ASGI scope for middleware/handlers."""
        scope["app"] = self

    async def handle_request(self, scope: dict, receive: Any, send: Any) -> None:
        """Delegate to appropriate conductor based on scope['type']."""
        scope_type = scope["type"]

        if scope_type == "http":
            await self.http_conductor.handle(scope, receive, send)
        elif scope_type == "lifespan":
            await self.lifespan_conductor.handle(scope, receive, send)
        elif scope_type == "websocket":
            await self.websocket_conductor.handle(scope, receive, send)
        else:
            # Unknown ASGI scope type, close connection
            await send({"type": "websocket.close", "code": 1002})
