"""
WebSocket Conductor for the Cara framework.

Orchestrates WebSocket connection handling with Laravel-style patterns.
Follows HttpConductor architecture with proper route resolution and middleware.
"""

from itertools import chain
from typing import Any, Dict

from cara.exceptions import MiddlewareNotFoundException
from cara.facades import Log
from cara.support import Pipeline
from cara.websocket import Socket


class WebsocketConductor:
    """
    Orchestrates WebSocket connections with modern patterns.

    Features:
    - Route resolution with 404 handling (before middleware)
    - Global and route-specific middleware pipeline
    - Proper auth cleanup and terminable middleware
    - Exception handling at application level
    """

    def __init__(self, application):
        """Initialize conductor with application instance."""
        self.application = application
        self.socket: Socket = None
        self.router = None

    async def handle(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        """
        Main entry point for WebSocket connections.

        Follows HttpConductor pattern:
        1. Initialize socket and router
        2. Let application-level exception handling work
        """
        self.router = self.application.router
        self.socket = Socket(self.application, scope, receive, send)
        await self.handle_request(scope, receive, send)

    async def handle_request(
        self, scope: Dict[str, Any], receive: Any, send: Any
    ) -> None:
        """
        Handle WebSocket request with Laravel-style lifecycle.

        Laravel-style pattern:
        1. Accept connection first
        2. Global middleware pipeline (runs even if route not found)
        3. Route resolution and route-specific middleware
        4. Controller dispatch
        5. Cleanup terminable middleware
        """
        # Accept connection first
        if not getattr(self.socket, "_ws_connected", False):
            await self.socket.accept()

        global_middleware = self.get_global_middleware()

        async def full_handler(socket: Socket):
            """Route matching, route-specific middleware and controller dispatch."""
            # Route matching happens AFTER global middleware (Laravel style)
            try:
                route = self.router.find(socket.path, "ws")
                socket.set_route(route).load_params(
                    route.set_params_from_path(socket.path)
                )
            except Exception as e:
                # If route not found or method not allowed, let exception handler deal with it
                raise e

            async def route_dispatch(s: Socket) -> None:
                """Dispatch to controller and handle the result."""
                result = await route.controller.handle((s, {}))
                # WebSocket controllers don't return responses like HTTP
                return result

            # Get and run route middleware
            route_middleware = self.get_route_middleware(route)
            return await Pipeline(socket, application=self.application).through(
                route_middleware
            )(route_dispatch)

        try:
            # Run the full pipeline with global middleware
            await Pipeline(self.socket, application=self.application).through(
                global_middleware
            )(full_handler)
        except Exception as e:
            Log.error(f"Error in WebSocket connection: {e}")
            raise
        finally:
            # Clean up socket broadcasting first
            await self.socket.cleanup_broadcasting()
            # Always run terminable middleware
            await self._run_terminable_middleware()

    def get_global_middleware(self):
        """
        Returns global WebSocket middleware.

        In Laravel style, global middleware runs before route matching,
        so no route-specific filtering is needed here.
        """
        capsule = self.application.make("middleware_ws")
        return capsule.get_global_middleware()

    def get_route_middleware(self, route):
        """
        Returns resolved route-specific middleware.

        If route middleware contains same class as global middleware,
        the route middleware (with parameters) takes priority.
        """
        capsule = self.application.make("middleware_ws")
        route_middleware = []

        for mw in route.get_middleware():
            resolved = capsule.resolve_middleware(mw)
            if resolved is None:
                raise MiddlewareNotFoundException(
                    f"Middleware alias or group '{mw}' could not be resolved."
                )
            if isinstance(resolved, list):
                route_middleware.extend(resolved)
            else:
                route_middleware.append(resolved)

        return route_middleware

    async def _run_terminable_middleware(self):
        """
        Calls terminate() on all terminable middleware after connection closes.
        Similar to HttpConductor but adapted for WebSocket context.
        """
        # Run auth cache cleanup first (if applicable)
        try:
            from cara.middleware.ws import ResetAuth

            reset_auth_middleware = ResetAuth(self.application)
            await reset_auth_middleware.terminate(self.socket)
        except ImportError:
            # ResetAuth might not exist for WebSocket, skip silently
            pass
        except Exception as e:
            Log.error(f"Critical error in WebSocket auth cache cleanup: {e}")

        # Then run other terminable middleware
        capsule = self.application.make("middleware_ws")
        all_middleware_classes = chain(
            capsule.get_global_middleware(),
            *(group_list for group_list in capsule._route_middleware.values()),
        )
        for mw_class in all_middleware_classes:
            try:
                instance = mw_class(self.application)
                terminate_fn = getattr(instance, "terminate", None)
                if callable(terminate_fn):
                    # Check if terminate method expects (socket) or (request, response)
                    import inspect

                    sig = inspect.signature(terminate_fn)
                    param_count = len(
                        [p for p in sig.parameters.values() if p.name != "self"]
                    )
                    if param_count == 1:  # socket only
                        await terminate_fn(self.socket)
                    elif param_count == 2:  # request, response (HTTP-style)
                        # Skip HTTP-style terminate methods for WebSocket
                        continue
            except Exception as e:
                Log.error(
                    f"Error in terminable WebSocket middleware {mw_class.__name__}: {e}"
                )
