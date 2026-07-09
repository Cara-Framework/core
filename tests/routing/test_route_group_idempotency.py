"""Regression pins for idempotent route grouping.

Grouping mutates Route objects in place. Re-running the same group over
the same objects (hot reload, repeated discovery, tests re-importing
module-level route definitions) used to stack the URL prefix, the name
prefix AND the group middleware a second time. Both grouping surfaces
(``Route.group`` classmethod and the fluent ``RouteGroup.routes``) now
record each application on the route and skip identical re-applications,
while different nested groups still stack.
"""

from __future__ import annotations

from cara.routing import Route
from cara.routing.RouteGroup import RouteGroup


def _controller(request, response):  # pragma: no cover — never invoked
    return response


class TestRouteGroupClassmethodIdempotency:
    def test_reapplying_same_group_does_not_double_prefix(self):
        route = Route.get("/users", _controller, name="users.index")

        Route.group(route, prefix="/api", name="api.")
        Route.group(route, prefix="/api", name="api.")

        assert route.url == "/api/users"
        assert route.get_name() == "api.users.index"

    def test_reapplying_same_group_does_not_duplicate_middleware(self):
        route = Route.get("/users", _controller).middleware("inner")

        Route.group(route, middleware="outer")
        Route.group(route, middleware="outer")

        assert route.get_middleware() == ["outer", "inner"]

    def test_different_nested_groups_still_stack(self):
        route = Route.get("/users", _controller)

        Route.group(route, prefix="/admin", middleware="admin_gate")
        Route.group(route, prefix="/api", middleware="throttle")

        assert route.url == "/api/admin/users"
        assert route.get_middleware() == ["throttle", "admin_gate"]

    def test_name_prefix_applies_even_when_name_shares_letters(self):
        # The old startswith() "idempotency" heuristic skipped legitimate
        # first applications when the route name coincidentally began
        # with the prefix text.
        route = Route.get("/users", _controller, name="apiusers")

        Route.group(route, name="api")

        assert route.get_name() == "apiapiusers"


class TestRouteGroupFluentIdempotency:
    def test_reapplying_same_fluent_group_is_noop(self):
        route = Route.get("/users", _controller).middleware("inner")
        group = RouteGroup(prefix="/api", middleware=["outer"])

        group.routes(route)
        group.routes(route)

        assert route.url == "/api/users"
        assert route.get_middleware() == ["outer", "inner"]

    def test_distinct_fluent_groups_still_stack(self):
        route = Route.get("/users", _controller)

        RouteGroup(prefix="/admin").routes(route)
        RouteGroup(prefix="/api").routes(route)

        assert route.url == "/api/admin/users"
