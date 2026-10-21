"""MiddlewareParameterParser: route strings reach annotated parameters.

Every middleware module uses ``from __future__ import annotations``, so the
``__init__`` signature carried the STRING ``'str'`` / ``'list[str] | None'``.
Converting through it raised ``TypeError``, the parser swallowed it for any
optional parameter, and the default won: ``can:listings.manage`` checked
``view``, ``auth:jwt`` used the default guard, ``ws.throttle:foo`` spent the
``ws_connect`` budget. A value that cannot be applied is a route declaration
bug and now raises instead of falling back.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cara.exceptions import ConfigurationException
from cara.middleware.http.CanPerform import CanPerform
from cara.middleware.http.ShouldAuthenticate import ShouldAuthenticate
from cara.middleware.Middleware import Middleware
from cara.middleware.ws.Authenticate import Authenticate
from cara.middleware.ws.Throttle import Throttle


def test_can_receives_ability_and_resource():
    middleware = CanPerform.create_with_parameters(MagicMock(), ["listings.manage", "x"])

    assert middleware.ability == "listings.manage"
    assert middleware.resource == "x"


def test_auth_receives_guards():
    middleware = ShouldAuthenticate.create_with_parameters(MagicMock(), ["jwt"])

    assert middleware.guards == ["jwt"]


def test_trailing_list_parameter_takes_every_remaining_value():
    middleware = ShouldAuthenticate.create_with_parameters(
        MagicMock(), ["jwt", "session"]
    )

    assert middleware.guards == ["jwt", "session"]


def test_ws_auth_and_ws_throttle_receive_their_values():
    assert Authenticate.create_with_parameters(MagicMock(), ["jwt"]).guards == ["jwt"]
    assert Throttle.create_with_parameters(MagicMock(), ["foo"]).name == "foo"


class _Counted(Middleware):
    def __init__(self, application, count: int = 5):
        super().__init__(application)
        self.count = count

    async def handle(self, request, next_fn):
        return await next_fn(request)


class _Required(Middleware):
    def __init__(self, application, name: str):
        super().__init__(application)
        self.name = name

    async def handle(self, request, next_fn):
        return await next_fn(request)


def test_unparseable_value_raises_instead_of_using_the_default():
    assert _Counted.create_with_parameters(MagicMock(), ["7"]).count == 7

    with pytest.raises(ConfigurationException, match="count"):
        _Counted.create_with_parameters(MagicMock(), ["seven"])


def test_missing_required_parameter_is_a_configuration_error():
    with pytest.raises(ConfigurationException, match="name"):
        _Required.create_with_parameters(MagicMock(), [])


@pytest.mark.asyncio
async def test_can_passes_route_id_parameters_to_the_gate():
    gate = MagicMock()
    gate.allows.return_value = True
    application = MagicMock()
    application.make.return_value = gate
    middleware = CanPerform(application, "orders.view")
    request = SimpleNamespace(params={"order_id": "ORD1", "tab": "lines"})

    async def next_fn(_request):
        return "ok"

    assert await middleware.handle(request, next_fn) == "ok"
    gate.allows.assert_called_once_with("orders.view", "ORD1")
