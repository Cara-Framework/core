"""``@head(...)`` must generate a route the router can build.

The docstring parser accepts ``@head`` as a verb, and the renderer emitted
``Route.head(...)`` for it — a classmethod ``Route`` has never had (``Route.get``
answers HEAD as well). The generated routes file raised ``AttributeError`` on
import, so one ``@head`` annotation took the whole API down at boot.
"""

from cara.commands.core.RouteGeneratorCommand import RouteGeneratorCommand
from cara.routing.Route import Route


def _render(http_method: str) -> str:
    command = object.__new__(RouteGeneratorCommand)
    return command._build_route_instance(
        {
            "http_method": http_method,
            "controller_method": "probe",
            "path": "/health",
            "as": "health.probe",
            "middleware": ["throttle:health"],
        },
        "HealthController",
    )


def test_head_is_rendered_through_the_factory() -> None:
    rendered = _render("head")
    assert rendered == (
        'Route.factory("/health", "HealthController@probe", ["head"], '
        'middleware=["throttle:health"], name="health.probe")'
    )


def test_every_other_verb_names_a_classmethod_route_really_has() -> None:
    for verb in ("get", "post", "put", "patch", "delete", "options", "ws"):
        rendered = _render(verb)
        assert rendered.startswith(f"Route.{verb}(")
        assert callable(getattr(Route, verb))
    assert callable(Route.factory)
    assert not hasattr(Route, "head")
