"""A raise behind a condition the route decides is listed only where it can run.

Review 2026-11-11 (fix4 openapi): the follower listed every raise on the call
path, so 175 reads and 64 billing-exempt writes published the 402 of a write
gate whose own condition never lets them through — ``authorize`` checks the
plan only for an unsafe HTTP method, a billed permission key and
``billing=True``. The seller API reference is built from this document, so a
status there is a promise. The follower now evaluates the conditions the
source pins: the routed request's method, a literal argument, a default, a
value handed on through a forwarding call (``run_in_thread(fn, *args)``).
Anything it cannot read stays a "can raise".
"""

from __future__ import annotations

from cara.openapi import ControllerContractExtractor, ControllerResponse
from tests.openapi._reachable_fixtures import (
    ROUTE_FRAMEWORK,
    ROUTE_STATUSES,
    route_errors,
    routes_for,
    tree,
)


def test_a_read_does_not_list_the_write_gate_its_method_skips(tmp_path):
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize


class ThingController:
    async def index(self, request: Request, response):
        authorize(request, "things.manage")
        return response.envelope([])

    async def store(self, request: Request, response):
        authorize(request, "things.manage")
        return response.envelope({})
""",
        },
    )
    routes = routes_for(index="GET", store="POST")

    assert route_errors(tmp_path, routes, "index") == {403}
    assert route_errors(tmp_path, routes, "store") == {402, 403}


def test_a_literal_flag_or_an_unbilled_key_keeps_the_gate_shut(tmp_path):
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize


class ThingController:
    async def ship(self, request: Request, response):
        authorize(request, "things.manage", billing=False)
        return response.envelope({})

    async def note(self, request: Request, response):
        authorize(request, "notes.manage")
        return response.envelope({})

    async def store(self, request: Request, response):
        authorize(request, "things.manage", billing=True)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(ship="POST", note="POST", store="POST")

    assert route_errors(tmp_path, routes, "ship") == {403}
    assert route_errors(tmp_path, routes, "note") == {403}
    assert route_errors(tmp_path, routes, "store") == {402, 403}


def test_a_forwarding_call_hands_its_arguments_to_the_callable(tmp_path):
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.context.ExecutionContext import ExecutionContext
from cara.http.request.Request import Request

from app.support.Access import authorize


class ThingController:
    async def ship(self, request: Request, response):
        await ExecutionContext.run_in_thread(
            authorize, request, "things.manage", billing=False
        )
        return response.envelope({})

    async def store(self, request: Request, response):
        await ExecutionContext.run_in_thread(authorize, request, "things.manage")
        return response.envelope({})

    async def index(self, request: Request, response):
        await self._gate(request, billing=True)
        return response.envelope([])

    async def _gate(self, request, *, billing):
        await ExecutionContext.run_in_thread(
            authorize, request, "things.manage", billing=billing
        )
""",
        },
    )
    routes = routes_for(ship="POST", store="POST", index="GET")

    assert route_errors(tmp_path, routes, "ship") == {403}
    assert route_errors(tmp_path, routes, "store") == {402, 403}
    # The method rides the request through ``_gate`` and the forwarder.
    assert route_errors(tmp_path, routes, "index") == {403}


def test_a_call_that_does_not_forward_is_not_given_the_arguments(tmp_path):
    # ``later`` keeps ``fn`` and calls it with nothing: what it hands over is
    # not what ``fn`` receives, so ``billing=False`` there proves nothing.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize


def later(fn, *args, **kwargs):
    return fn()


class ThingController:
    async def ship(self, request: Request, response):
        later(authorize, request, "things.manage", billing=False)
        return response.envelope({})
""",
        },
    )

    assert route_errors(tmp_path, routes_for(ship="POST"), "ship") == {402, 403}


def test_a_guard_return_ends_the_path_and_the_method_attribute_is_read(tmp_path):
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.exceptions import ConflictException
from app.support.Access import billing_writable_request


def gate(request, billing=True):
    if not billing:
        return
    billing_writable_request(request)


class ThingController:
    async def ship(self, request: Request, response):
        gate(request, False)
        return response.envelope({})

    async def store(self, request: Request, response):
        gate(request)
        return response.envelope({})

    async def index(self, request: Request, response):
        if request.method == "DELETE":
            raise ConflictException("Busy.")
        return response.envelope([])

    async def destroy(self, request: Request, response):
        if request.method == "DELETE":
            raise ConflictException("Busy.")
        return response.no_content()
""",
        },
    )
    routes = routes_for(ship="POST", store="POST", index="GET", destroy="DELETE")

    assert route_errors(tmp_path, routes, "ship") == set()
    assert route_errors(tmp_path, routes, "store") == {402}
    assert route_errors(tmp_path, routes, "index") == set()
    assert route_errors(tmp_path, routes, "destroy") == {409}


def test_a_value_the_source_does_not_pin_stays_a_can_raise(tmp_path):
    # A rebound parameter, a lambda that names its own ``request`` and an
    # action no route reaches are not evaluated: their raise stays listed.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize, billing_writable_request


def gate(request, billing=True):
    billing = billing or lookup()
    if billing:
        billing_writable_request(request)


def quiet(request, billing=False):
    if billing:
        billing_writable_request(request)


class ThingController:
    async def ship(self, request: Request, response):
        gate(request, False)
        return response.envelope({})

    async def spread(self, request: Request, response):
        quiet(request, *self.flags)
        return response.envelope({})

    async def unpack(self, request: Request, response):
        quiet(request, **self.options)
        return response.envelope({})

    async def index(self, request: Request, response):
        each(lambda request: authorize(request, "things.manage"))
        return response.envelope([])

    async def store(self, request: Request, response):
        authorize(request, "things.manage")
        return response.envelope({})
""",
        },
    )
    routes = routes_for(ship="POST", index="GET", spread="POST", unpack="POST")

    assert route_errors(tmp_path, routes, "ship") == {402}
    assert route_errors(tmp_path, routes, "index") == {402, 403}
    assert route_errors(tmp_path, routes, "store") == {402, 403}
    # An expansion may fill ``billing``: its ``False`` default is not pinned.
    assert route_errors(tmp_path, routes, "spread") == {402}
    assert route_errors(tmp_path, routes, "unpack") == {402}


def test_a_decorator_or_a_reshaping_forwarder_pins_nothing(tmp_path):
    # ``injects`` calls ``quiet`` with ``billing=True`` whatever its caller
    # passed; ``strip`` drops ``billing`` before handing the rest on.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize, billing_writable_request


def injects(fn):
    def wrapper(*args, **kwargs):
        return fn(*args, billing=True, **kwargs)

    return wrapper


@injects
def quiet(request, billing=False):
    if billing:
        billing_writable_request(request)


def strip(fn, *args, **kwargs):
    kwargs.pop("billing", None)
    return fn(*args, **kwargs)


class ThingController:
    async def decorated(self, request: Request, response):
        quiet(request)
        return response.envelope({})

    async def reshaped(self, request: Request, response):
        strip(authorize, request, "things.manage", billing=False)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(decorated="POST", reshaped="POST")

    assert route_errors(tmp_path, routes, "decorated") == {402}
    assert route_errors(tmp_path, routes, "reshaped") == {402, 403}


def test_equal_values_of_two_types_are_two_facts(tmp_path):
    # ``True == 1`` and they hash alike: one cached reading for both would
    # answer ``is True`` wrongly for one of the two callers.
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.exceptions import ConflictException


def strict(request, flag):
    if flag is True:
        raise ConflictException("Busy.")


class ThingController:
    async def one(self, request: Request, response):
        strict(request, 1)
        return response.envelope({})

    async def yes(self, request: Request, response):
        strict(request, True)
        return response.envelope({})
""",
        },
    )
    routes = routes_for(one="POST", yes="POST")

    assert route_errors(tmp_path, routes, "one") == set()
    assert route_errors(tmp_path, routes, "yes") == {409}


def test_an_action_on_several_methods_lists_what_any_of_them_can_raise(tmp_path):
    tree(
        tmp_path,
        {
            **ROUTE_FRAMEWORK,
            "app/controllers/ThingController.py": """
from cara.http.request.Request import Request

from app.support.Access import authorize


class ThingController:
    async def upsert(self, request: Request, response):
        authorize(request, "things.manage")
        return response.envelope({})
""",
        },
    )
    routes = [
        {"method": "GET", "controller": "ThingController", "action": "upsert"},
        {"method": "PUT", "controller": "ThingController", "action": "upsert"},
    ]

    assert route_errors(tmp_path, routes, "upsert") == {402, 403}
    assert ControllerResponse(402, "error") not in (
        ControllerContractExtractor(
            tmp_path / "app" / "controllers", ROUTE_STATUSES, routes=routes[:1]
        )
        .extract()["ThingController@upsert"]
        .responses
    )
