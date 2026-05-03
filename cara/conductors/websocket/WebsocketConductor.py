"""
WebSocket Conductor for the Cara framework.

Orchestrates WebSocket connection handling with Laravel-style patterns.
Follows HttpConductor architecture with proper route resolution and middleware.
"""

from typing import Any, Dict

from cara.exceptions import MiddlewareNotFoundException, WebSocketException
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

        # Pipelines are tracked here so the terminable-middleware sweep
        # can call ``terminate()`` on the EXACT instances that ran for
        # this request. Without this we either (a) miss state-bearing
        # instances or (b) instantiate fresh ones, both of which
        # silently drop accumulated per-request bookkeeping.
        global_pipeline_holder: list = []
        route_pipeline_holder: list = []

        async def full_handler(socket: Socket):
            """Route matching, route-specific middleware and controller dispatch."""
            # Route matching happens AFTER global middleware (Laravel style)
            try:
                route = self.router.find(socket.path, "ws")
                socket.set_route(route).load_params(
                    route.set_params_from_path(socket.path)
                )
            except Exception:
                # If route not found or method not allowed, let exception handler deal with it
                raise

            async def route_dispatch(s: Socket) -> None:
                """Dispatch to controller and handle the result."""
                result = await route.controller.handle((s, {}))
                # WebSocket controllers don't return responses like HTTP
                return result

            # Get and run route middleware
            route_middleware = self.get_route_middleware(route)
            route_pipeline = Pipeline(socket, application=self.application).through(
                route_middleware
            )
            route_pipeline_holder.append(route_pipeline)
            return await route_pipeline(route_dispatch)

        # Track whether the controller ran cleanly so the close-code
        # below reflects reality. ``finally`` always runs but we want
        # 1000 (normal closure) on success and 1011 (internal error)
        # only on a real exception.
        clean_exit = False
        try:
            # Run the full pipeline with global middleware
            global_pipeline = Pipeline(self.socket, application=self.application).through(
                global_middleware
            )
            global_pipeline_holder.append(global_pipeline)
            await global_pipeline(full_handler)
            clean_exit = True
        except WebSocketException as e:
            # Known/expected WS errors (e.g. client-close race on send → 4002).
            # These are benign — client dropped mid-handler. Log at debug only.
            Log.debug(
                f"WebSocket connection ended: {e}",
                category="cara.websocket",
            )
            # WebSocketException carries its own close code (4xxx). Use
            # it directly so the client sees an accurate reason rather
            # than the catch-all 1011.
            self._wsx_close_code = getattr(e, "code", 1011)
            raise
        except Exception as e:
            Log.error(f"Error in WebSocket connection: {e}")
            raise
        finally:
            # Clean up socket broadcasting first
            await self.socket.cleanup_broadcasting()
            # Ensure connection is closed so the ASGI server releases it.
            # Pick the close code based on how we got here:
            #   1000 — controller returned normally
            #   4xxx — middleware/auth raised a typed WebSocketException
            #          (origin denied = 4003, unauthorized = 4006, etc.)
            #   1011 — anything else (uncaught controller exception)
            if not getattr(self.socket, "_closed", False):
                if clean_exit:
                    code, reason = 1000, ""
                else:
                    code = getattr(self, "_wsx_close_code", 1011)
                    reason = "Internal error" if code == 1011 else ""
                try:
                    await self.socket.close(code, reason)
                except Exception:
                    pass  # Already closed by client or transport
            # Always run terminable middleware on the EXACT instances
            # that executed for this request — same contract as the
            # HTTP conductor. See _run_terminable_middleware docstring.
            try:
                await self._run_terminable_middleware(
                    global_pipeline_holder[0] if global_pipeline_holder else None,
                    route_pipeline_holder[0] if route_pipeline_holder else None,
                )
            except Exception as term_exc:
                Log.error(
                    f"WebSocket terminable middleware sweep failed: {term_exc}",
                    category="cara.websocket",
                )

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

        # Apply Laravel-style priority ordering
        return capsule.sort_by_priority(route_middleware)

    async def _run_terminable_middleware(
        self,
        global_pipeline: Pipeline | None,
        route_pipeline: Pipeline | None,
    ) -> None:
        """
        Calls terminate() on the middleware instances that ACTUALLY ran
        for this request — same contract as ``HttpConductor``.

        The previous implementation walked ``capsule._route_middleware``
        (the registry of all aliases, not what executed) and instantiated
        fresh middleware per terminate call, which (a) silently terminated
        middleware bound to other routes and (b) lost state any middleware
        accumulated during ``handle()``. Tracking the executed instances
        on each Pipeline is the same fix we did on HTTP.
        """
        # Always run auth cleanup first — even on the failure path
        # where the auth middleware may not have completed. Mirrors
        # HttpConductor's ResetAuth handling.
        try:
            from cara.middleware.ws import ResetAuth

            await ResetAuth(self.application).terminate(self.socket)
        except ImportError:
            # ResetAuth ws variant not present in this build — fine.
            pass
        except Exception as e:
            Log.error(
                f"Critical error in WebSocket auth cache cleanup: {e}",
                category="cara.websocket",
            )

        # Collect the middleware instances that actually ran. The
        # Pipeline records each instance it walks through into
        # ``executed_instances`` — see Pipeline.through(...).
        executed: list = []
        if global_pipeline is not None:
            executed.extend(getattr(global_pipeline, "executed_instances", []))
        if route_pipeline is not None:
            executed.extend(getattr(route_pipeline, "executed_instances", []))

        # Deduplicate by identity — the same instance (e.g. one that
        # appears in both global and route lists) must not terminate
        # twice. Skip ResetAuth, already handled above.
        seen: set = set()
        for instance in executed:
            if id(instance) in seen:
                continue
            seen.add(id(instance))

            try:
                from cara.middleware.ws import ResetAuth as _ResetAuth

                if isinstance(instance, _ResetAuth):
                    continue
            except ImportError:
                pass

            terminate_fn = getattr(instance, "terminate", None)
            if not callable(terminate_fn):
                continue

            try:
                await terminate_fn(self.socket)
            except Exception as e:
                Log.error(
                    f"Error in terminable WebSocket middleware "
                    f"{type(instance).__name__}: {e}",
                    category="cara.websocket",
                )
