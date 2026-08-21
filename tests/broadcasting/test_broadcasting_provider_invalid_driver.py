"""BroadcastingProvider must refuse to boot on an unknown default driver.

``register()`` starts with a truthy check on ``broadcasting.default``, which
a misspelt value passes happily — ``BROADCAST_DRIVER=rabbit`` (the stale
value once shipped in ``.env.example``) is a perfectly truthy string. Without
a second check the mismatch stays invisible until the first
``Broadcast.fire(...)``, where ``Broadcasting.driver()`` raises
"Broadcasting driver 'rabbit' is not registered" — typically hours into a
workload, long after the process looked healthy at deploy.

The provider therefore re-checks the default against the drivers it actually
wired up, and the exception must name BOTH the bad value and the sorted set
of registered drivers so the operator can fix the typo without grepping cara.
Channel-route loading is pinned separately in
``test_channel_route_loading.py``; these tests only reach it incidentally and
therefore ISOLATE it. Leaving it live made the verdict depend on the working
directory: run from ``commons/cara`` no ``routes`` package is importable and
the positive control passed, but run the sanctioned way — from a deployable,
``cd api && pytest ../commons/cara/tests`` — ``routes.broadcasting`` resolves
to that product's real module, whose module-level ``@Broadcast.channel(...)``
decorator needs a booted container, and the control failed for a reason that
has nothing to do with the driver contract it exists to prove.
"""

from __future__ import annotations

import pytest

from cara.broadcasting import BroadcastingProvider
from cara.configuration import Configuration
from cara.exceptions import BroadcastingConfigurationException

ALL_DRIVERS = ("log", "memory", "null", "redis")


class _FakeApp:
    """Minimal Application stand-in exposing only what the provider touches.

    ``BroadcastingProvider.register()`` calls ``bind(...)`` once, and
    ``Broadcasting.add_driver`` appends each driver's ``cleanup`` hook to
    ``_shutdown_callbacks`` (creating the list itself when absent). Nothing
    else of the container is reached, so the full foundation bootstrap stays
    out of these tests.
    """

    def __init__(self) -> None:
        self.bound: dict[str, object] = {}

    def bind(self, key: str, value: object) -> None:
        """Record a container binding."""
        self.bound[key] = value


@pytest.fixture()
def isolated_config():
    """Install a fresh Configuration singleton for the duration of one test.

    ``Configuration()`` requires an application, so ``Configuration.empty()``
    is the framework's explicit authority for isolated tests. The restore
    runs in ``finally`` — leaking a torn-down singleton would poison every
    later ``config()`` call in the suite with "Configuration is unavailable".
    """
    saved = Configuration._instance
    try:
        yield Configuration.empty()
    finally:
        Configuration._instance = saved


@pytest.fixture(autouse=True)
def isolated_channel_routes(monkeypatch):
    """Keep the ambient ``routes`` package out of the driver contract.

    ``register()`` ends by loading the app's optional channel routes. That is
    a different contract with its own tests, and importing whichever product
    happens to be on ``sys.path`` would decide these tests' outcome.
    """
    monkeypatch.setattr(
        BroadcastingProvider, "_load_channel_routes", staticmethod(lambda: None)
    )


def _wire_all_drivers(cfg: Configuration) -> None:
    """Configure all four real broadcasting drivers with usable settings."""
    cfg.set(
        "broadcasting.drivers.redis",
        {
            "driver": "redis",
            "connection": {"host": "127.0.0.1", "port": 6379, "password": "", "db": 0},
        },
    )
    cfg.set("broadcasting.drivers.memory", {"driver": "memory"})
    cfg.set("broadcasting.drivers.log", {"driver": "log"})
    cfg.set("broadcasting.drivers.null", {"driver": "null"})


def test_unknown_default_driver_fails_at_register(isolated_config) -> None:
    """A truthy-but-unregistered BROADCAST_DRIVER must abort registration."""
    isolated_config.set("broadcasting.default", "rabbit")
    _wire_all_drivers(isolated_config)
    app = _FakeApp()

    with pytest.raises(BroadcastingConfigurationException) as excinfo:
        BroadcastingProvider(app).register()

    message = str(excinfo.value)
    assert "rabbit" in message, message
    for driver in ALL_DRIVERS:
        assert driver in message, (
            f"the error must list {driver!r} as an available driver so the "
            f"operator can fix BROADCAST_DRIVER from the message alone: {message}"
        )
    assert "broadcasting" not in app.bound, (
        "a provider that raised must not have bound a half-built manager"
    )


def test_missing_default_driver_fails_at_register(isolated_config) -> None:
    """An absent ``broadcasting.default`` still trips the original guard."""
    _wire_all_drivers(isolated_config)

    with pytest.raises(BroadcastingConfigurationException, match="must be specified"):
        BroadcastingProvider(_FakeApp()).register()


def test_valid_default_driver_binds_the_manager(isolated_config) -> None:
    """Positive control: a registered default binds a manager holding it.

    Without this the negative test above could pass for the wrong reason —
    e.g. every ``register()`` raising because the fake app is too thin.
    """
    isolated_config.set("broadcasting.default", "memory")
    _wire_all_drivers(isolated_config)
    app = _FakeApp()

    BroadcastingProvider(app).register()

    manager = app.bound["broadcasting"]
    assert manager.default_driver == "memory"
    assert sorted(manager._drivers) == list(ALL_DRIVERS)
    assert manager.driver() is manager._drivers["memory"]
