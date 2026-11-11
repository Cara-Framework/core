"""A fact is trusted only where Python would compute the same value.

Review 2026-11-11 (fix5 openapi, adversarial pass over the route facts): a fact
the follower reads wrongly drops a status the operation really answers, and a
fact it cannot read keeps one it never answers. These pin the edges: a test
runs its calls only as far as Python evaluates it; a value the callee can
change in place, or a name a ``match`` pattern rebinds, is no fact; a
conditional expression and a name used inside an argument expression are read.
"""

from __future__ import annotations

from tests.openapi._reachable_fixtures import (
    ROUTE_FRAMEWORK,
    route_errors,
    routes_for,
    tree,
)


def test_a_test_runs_its_calls_only_as_far_as_python_evaluates_it(tmp_path):
    # ``and`` stops at its first falsy operand and ``or`` at its first truthy
    # one, so the gate behind a decided operand never runs. Nothing pinned
    # this: walking every call of the test kept the suite green.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import billing_writable_request


def both(request, billing=True):
    if billing and billing_writable_request(request):
        return


def either(request, billing=True):
    if not billing or billing_writable_request(request):
        return


class ThingController:
    async def ship(self, request: Request, response):
        both(request, billing=False)
        either(request, billing=False)
        return response.envelope({})

    async def store(self, request: Request, response):
        both(request)
        return response.envelope({})

    async def update(self, request: Request, response):
        either(request)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(ship="POST", store="POST", update="POST")

    assert route_errors(tmp_path, routes, "ship") == set()
    assert route_errors(tmp_path, routes, "store") == {402}
    assert route_errors(tmp_path, routes, "update") == {402}


def test_a_value_the_callee_can_change_in_place_pins_nothing(tmp_path):
    # A list or set display is a new mutable object, so ``holds.append(...)``
    # changes it without rebinding the name, and a list is never equal to the
    # tuple it used to be read as. Only immutable values (scalars, tuples of
    # them, the routed request) are facts.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.exceptions import ConflictException
from app.support.Access import billing_writable_request


def grows(request, holds):
    holds.append("billing")
    if holds:
        billing_writable_request(request)


def shrinks(request, holds):
    holds.discard("billing")
    if not holds:
        billing_writable_request(request)


def differs(request, keys):
    if keys != ("a",):
        raise ConflictException("Busy.")


class ThingController:
    async def store(self, request: Request, response):
        grows(request, [])
        return response.envelope({})

    async def update(self, request: Request, response):
        shrinks(request, {"billing"})
        return response.envelope({})

    async def listed(self, request: Request, response):
        differs(request, ["a"])
        return response.envelope({})

    async def tupled(self, request: Request, response):
        differs(request, ("a",))
        return response.envelope({})
""",
        },
    )
    routes = routes_for(store="POST", update="POST", listed="POST", tupled="POST")

    assert route_errors(tmp_path, routes, "store") == {402}
    assert route_errors(tmp_path, routes, "update") == {402}
    # ``["a"] != ("a",)`` is True: the 409 runs.
    assert route_errors(tmp_path, routes, "listed") == {409}
    assert route_errors(tmp_path, routes, "tupled") == set()


def test_a_value_built_from_facts_is_read_where_it_is_handed_on(tmp_path):
    # ``bulk.operations.revert`` and ``automations.simulate`` published 402
    # although neither can answer it. Revert reads its operation with
    # ``write=False``, which picks the view key (``"catalog.manage" if write
    # else "catalog.view"``); simulate hands on ``billing=billing and
    # demand.billing`` with ``billing=False``. A conditional expression was
    # not read, and a name used only inside an argument expression was not
    # kept as a fact.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize


def scope_gate(request, *, write, billing=True):
    authorize(request, "things.manage" if write else "things.view", billing=billing)


def door_gate(request, *, billing=True):
    authorize(request, "things.manage", billing=billing and lookup())


class ThingController:
    async def revert(self, request: Request, response):
        scope_gate(request, write=False)
        return response.envelope({})

    async def store(self, request: Request, response):
        scope_gate(request, write=True)
        return response.envelope({})

    async def simulate(self, request: Request, response):
        door_gate(request, billing=False)
        return response.envelope({})

    async def update(self, request: Request, response):
        door_gate(request)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(revert="POST", store="POST", simulate="POST", update="POST")

    assert route_errors(tmp_path, routes, "revert") == {403}
    assert route_errors(tmp_path, routes, "store") == {402, 403}
    assert route_errors(tmp_path, routes, "simulate") == {403}
    # ``True and lookup()`` is whatever ``lookup()`` returns: not decided.
    assert route_errors(tmp_path, routes, "update") == {402, 403}


def test_a_method_reached_through_the_class_takes_its_receiver_as_written(
    tmp_path,
):
    # ``cls.check(gates, 0, True)`` in a classmethod passes ``check``'s
    # ``self`` itself; reading it as a bound call shifted every argument one
    # parameter left, so ``billing`` read the ``0`` meant for ``retries``.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import billing_writable_request


class Gates:
    def check(self, retries, billing):
        if billing:
            billing_writable_request(None)

    @classmethod
    def run(cls, gates):
        cls.check(gates, 0, True)

    @classmethod
    def build(cls, flag):
        if flag:
            billing_writable_request(None)

    @classmethod
    def twice(cls):
        cls.build(False)


class ThingController:
    async def store(self, request: Request, response):
        Gates.run(request)
        return response.envelope({})

    async def update(self, request: Request, response):
        Gates.twice()
        return response.envelope({})
""",
        },
    )
    routes = routes_for(store="POST", update="POST")

    assert route_errors(tmp_path, routes, "store") == {402}
    # A classmethod through ``cls`` still gets its own ``cls`` from the call.
    assert route_errors(tmp_path, routes, "update") == set()


def test_a_name_a_match_pattern_captures_is_rebound(tmp_path):
    # ``case {"billing": billing}`` binds ``billing`` with no ``Name`` store,
    # so the default ``False`` was kept past it and the gate below dropped.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import billing_writable_request


def captured(request, billing=False):
    match lookup():
        case {"billing": billing}:
            pass
    if billing:
        billing_writable_request(request)


def starred(request, billing=False):
    match lookup():
        case [*billing]:
            pass
    if billing:
        billing_writable_request(request)


def rest(request, billing=False):
    match lookup():
        case {"a": 1, **billing}:
            pass
    if billing:
        billing_writable_request(request)


class ThingController:
    async def store(self, request: Request, response):
        captured(request)
        return response.envelope({})

    async def update(self, request: Request, response):
        starred(request)
        return response.envelope({})

    async def patch(self, request: Request, response):
        rest(request)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(store="POST", update="POST", patch="POST")

    assert route_errors(tmp_path, routes, "store") == {402}
    assert route_errors(tmp_path, routes, "update") == {402}
    assert route_errors(tmp_path, routes, "patch") == {402}
