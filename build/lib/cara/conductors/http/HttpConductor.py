"""
HTTP Request Conductor.

This module handles all HTTP traffic in the Cara framework. It orchestrates the flow of HTTP
requests through the framework, managing routing, middleware, and response handling.
"""

from itertools import chain
from typing import Any, Dict

from cara.exceptions import MiddlewareNotFoundException
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware.http import ResetAuth
from cara.support import Pipeline


class HttpConductor:
    """
    Orchestrates HTTP request handling:
    - Loads request/response objects
    - Finds route first (404 check)
    - Runs global and route-specific middleware
    - Dispatches controller and returns response
    """

    def __init__(self, application):
        """Initialize conductor with application instance."""
        self.application = application
        self.request: Request = None
        self.response: Response = None
        self.router = None

    async def handle(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        """
        Main entry for HTTP requests.
        """
        self.request = self.application.make("request")
        self.response = self.application.make("response")
        # Router is already initialized at startup, just get it from application
        self.router = self.application.router
        await self.handle_request(scope, receive, send)

    async def handle_request(
        self, scope: Dict[str, Any], receive: Any, send: Any
    ) -> None:
        """
        Main HTTP request flow (Laravel style):
        1. Load ASGI scope into Request
        2. Run global middleware pipeline first
        3. Find route and run route-specific middleware
        4. Dispatch controller and return response
        5. Send response via ASGI
        """
        self.request.load(scope, receive)

        global_middleware = self.get_global_middleware()

        async def full_handler(req: Request):
            """
            Route matching, route-specific middleware and controller dispatch.
            """
            # Route matching happens AFTER global middleware (Laravel style)
            try:
                route = self.router.find(req.path, req.method)
                req.set_route(route).load_params(route.set_params_from_path(req.path))
            except Exception as e:
                # If route not found or method not allowed, let exception handler deal with it
                raise e

            async def route_dispatch(r: Request) -> Response:
                """
                Dispatch to controller and handle the result.
                """
                result = await route.controller.handle((r, self.response))
                if isinstance(result, type(self.response)):
                    self.response.clone_from(result)
                elif result is not None:
                    self.response.json(result)
                # Always return a Response object
                return self.response

            route_middleware = self.get_route_middleware(route)
            return await Pipeline(req, application=self.application).through(
                route_middleware
            )(route_dispatch)

        try:
            response = await Pipeline(self.request, application=self.application).through(
                global_middleware
            )(full_handler)
        except Exception as e:
            Log.error(f"Error in HTTP connection: {e}")
            raise

        await self.maybe_send_response(scope, receive, send, response)

    def get_global_middleware(self):
        """
        Returns global HTTP middleware.

        In Laravel style, global middleware runs before route matching,
        so no route-specific filtering is needed here.
        """
        capsule = self.application.make("middleware_http")
        return capsule.get_global_middleware()

    def get_route_middleware(self, route):
        """
        Returns resolved route-specific middleware.

        If route middleware contains same class as global middleware,
        the route middleware (with parameters) takes priority.
        """
        capsule = self.application.make("middleware_http")
        route_middleware = []
        global_middleware_classes = set()

        # Track global middleware classes to detect duplicates
        for global_mw in capsule.get_global_middleware():
            # Get base class (in case of parameterized middleware)
            base_class = (
                getattr(global_mw, "__bases__", [global_mw])[0]
                if hasattr(global_mw, "__bases__")
                else global_mw
            )
            global_middleware_classes.add(base_class)

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

    async def maybe_send_response(self, scope, receive, send, response=None):
        """
        Sends the response via ASGI if not already sent.
        Also runs terminate() on any terminable middleware.
        """
        if not scope.get("response_sent") and not (response or self.response).is_sent():
            await (response or self.response)(scope, receive, send)
            scope["response_sent"] = True
            await self._run_terminable_middleware(self.request, response or self.response)

    async def _run_terminable_middleware(self, request: Request, response: Response):
        """
        Calls terminate() on all terminable middleware after response is sent.
        """
        # CRITICAL: Always run auth cache cleanup first
        reset_auth_middleware = ResetAuth(self.application)
        try:
            await reset_auth_middleware.terminate(request, response)
        except Exception as e:
            Log.error(f"Critical error in auth cache cleanup: {e}")

        # Then run other terminable middleware
        capsule = self.application.make("middleware_http")
        all_middleware_classes = chain(
            capsule.get_global_middleware(),
            *(group_list for group_list in capsule._route_middleware.values()),
        )
        for mw_class in all_middleware_classes:
            try:
                instance = mw_class(self.application)
                terminate_fn = getattr(instance, "terminate", None)
                if callable(terminate_fn):
                    await terminate_fn(request, response)
            except Exception as e:
                Log.error(f"Error in terminable middleware {mw_class.__name__}: {e}")
