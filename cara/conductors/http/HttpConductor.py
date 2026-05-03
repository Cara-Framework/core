"""
HTTP Request Conductor.

This module handles all HTTP traffic in the Cara framework. It orchestrates the flow of HTTP
requests through the framework, managing routing, middleware, and response handling.

CRITICAL FIX (2026-04-27): This conductor is a singleton — one instance handles
ALL concurrent requests. Previous code stored request, response, and pipeline
objects as ``self.*`` attributes, so concurrent requests overwrote each other's
state. Symptoms: "ASGI callable returned without starting response" errors and
random empty/wrong responses under concurrent load.

Fix: all per-request state is now kept in local variables and passed through
closures / function arguments. The conductor instance carries only the
application reference and router (both immutable across requests).
"""

from __future__ import annotations

from typing import Any, Dict, List

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

    IMPORTANT: This is a singleton — all per-request state MUST be local
    variables, never stored on ``self``.
    """

    def __init__(self, application):
        """Initialize conductor with application instance."""
        self.application = application
        self.router = None

    async def handle(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        """
        Main entry for HTTP requests.

        Creates per-request Request and Response objects as LOCAL variables
        (not self.*) so concurrent requests cannot interfere with each other.
        """
        request = self.application.make("request")
        response = self.application.make("response")
        # Router is already initialized at startup, just get it from application
        if self.router is None:
            self.router = self.application.router
        await self._handle_request(scope, receive, send, request, response)

    async def _handle_request(
        self,
        scope: Dict[str, Any],
        receive: Any,
        send: Any,
        request: Request,
        response: Response,
    ) -> None:
        """
        Main HTTP request flow (Laravel style):
        1. Load ASGI scope into Request
        2. Run global middleware pipeline first
        3. Find route and run route-specific middleware
        4. Dispatch controller and return response
        5. Send response via ASGI

        All state is per-request (passed as arguments or captured in closures).
        """
        request.load(scope, receive)

        global_middleware = self.get_global_middleware()

        # Track pipelines so terminate() can reuse the actual middleware
        # instances that processed the request (not fresh copies).
        # These are LOCAL — not stored on self.
        global_pipeline = Pipeline(request, application=self.application)
        route_pipeline_holder: List[Pipeline] = []  # mutable container for closure

        async def full_handler(req: Request):
            """
            Route matching, route-specific middleware and controller dispatch.
            """
            # Route matching happens AFTER global middleware (Laravel style)
            try:
                route = self.router.find(req.path, req.method)
                req.set_route(route).load_params(route.set_params_from_path(req.path))
            except Exception:
                # If route not found or method not allowed, let exception handler deal with it
                raise

            async def route_dispatch(r: Request) -> Response:
                """Dispatch to controller and handle the result."""
                result = await route.controller.handle((r, response))
                if isinstance(result, type(response)):
                    response.clone_from(result)
                elif hasattr(result, "to_response"):
                    resource_response = result.to_response(response)
                    if resource_response is not response:
                        response.clone_from(resource_response)
                elif result is not None:
                    response.json(result)
                return response

            route_middleware = self.get_route_middleware(route)
            rp = Pipeline(req, application=self.application)
            route_pipeline_holder.append(rp)
            return await rp.through(
                route_middleware
            )(route_dispatch)

        final_response: Response | None = None
        try:
            final_response = await global_pipeline.through(
                global_middleware
            )(full_handler)
        except Exception as e:
            Log.error(f"Error in HTTP connection: {e}")
            raise
        finally:
            # CRITICAL: terminate middleware MUST run on every code path,
            # including the exception path. Previously the terminate
            # block lived after the try/except, so any exception in the
            # pipeline (auth guard rejecting a request, route not found,
            # body parse error, ...) skipped terminate entirely. ResetAuth
            # is the most-impactful one — without it, the next request on
            # the same worker observes the previous request's user state
            # because the auth singleton's ``_user`` field is still set.
            #
            # We still send the response in the success path below; this
            # finally block exists purely to guarantee terminate runs.
            try:
                resp_for_term = final_response or response
                route_pipeline = route_pipeline_holder[0] if route_pipeline_holder else None
                await self._run_terminable_middleware(
                    request, resp_for_term, global_pipeline, route_pipeline
                )
            except Exception as term_exc:
                # Never let a terminate failure mask the real exception.
                Log.error(
                    f"Terminable middleware sweep failed: {term_exc}",
                )

        # Send response via ASGI
        resp = final_response or response
        if not scope.get("response_sent") and not resp.is_sent():
            await resp(scope, receive, send)
            scope["response_sent"] = True

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

        Sort order is left to ``MiddlewareCapsule.sort_by_priority`` so
        global / route conflict resolution stays in the capsule's
        contract, not duplicated here.
        """
        capsule = self.application.make("middleware_http")
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
        return capsule.sort_by_priority(route_middleware)

    async def _run_terminable_middleware(
        self,
        request: Request,
        response: Response,
        global_pipeline: Pipeline,
        route_pipeline: Pipeline | None,
    ):
        """
        Calls terminate() on all terminable middleware after response is sent.

        Uses the actual middleware instances that processed the request
        (tracked by the Pipeline) rather than creating fresh copies. This
        ensures terminate() sees any state the middleware accumulated
        during handle().

        Pipelines are passed as arguments (not read from self) to maintain
        per-request isolation.
        """
        # CRITICAL: Always run auth cache cleanup first
        reset_auth_middleware = ResetAuth(self.application)
        try:
            await reset_auth_middleware.terminate(request, response)
        except Exception as e:
            Log.error(f"Critical error in auth cache cleanup: {e}")

        # Collect instances that actually executed during this request.
        executed: list = []
        if global_pipeline:
            executed.extend(global_pipeline.executed_instances)
        if route_pipeline:
            executed.extend(route_pipeline.executed_instances)

        # Deduplicate by identity (same instance should not terminate twice).
        seen_ids: set = set()
        for instance in executed:
            if id(instance) in seen_ids:
                continue
            seen_ids.add(id(instance))

            # Skip ResetAuth — already handled above.
            if isinstance(instance, ResetAuth):
                continue

            terminate_fn = getattr(instance, "terminate", None)
            if terminate_fn is None or not callable(terminate_fn):
                continue

            try:
                await terminate_fn(request, response)
            except Exception as e:
                name = type(instance).__name__
                Log.error(f"Error in terminable middleware {name}: {e}")
