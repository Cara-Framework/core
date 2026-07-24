"""Pin the ``broadcasting.websocket`` config path (lowercase).

``Configuration.load`` lower-cases every module attribute name when
materialising config ("<module>.<name.lower()>"), so the ``WEBSOCKET``
dict declared in ``config/broadcasting.py`` is ONLY reachable at
``broadcasting.websocket``. A previous generation of readers looked up
the uppercase ``broadcasting.WEBSOCKET`` path, which silently resolved
to ``None``/``{}`` forever: operator env knobs (per-user connection
caps, subscription caps, the WS origin allow-list) were swallowed at
boot with no error — the exact footgun already fixed once for
``rate.trusted_ips`` in ``ThrottleRequests``.

This file pins both halves of the contract:

1. ``Configuration.load`` really does materialise uppercase module
   attributes under the lowercase dotted path (and NOT the uppercase one).
2. The runtime readers (``Socket._max_subscriptions``,
   ``ws.Authenticate._origin_is_allowed``) resolve their knobs from that
   lowercase subtree, and no cara source ever regresses to the
   uppercase path.
"""

from __future__ import annotations

import builtins
import types
from pathlib import Path

import pytest

from cara.configuration import Configuration
from cara.loader.Loader import Loader as RealLoader

CARA_SRC = Path(__file__).resolve().parent.parent.parent / "cara"

WS_SETTINGS = {
    "max_subscriptions_per_connection": 7,
    "max_connections_per_user": 3,
    "allowed_origins": ["https://app.example"],
}


class _StubLoader:
    """Feed in-memory config modules through the REAL parameter-extraction
    and storage code paths (only directory scanning is stubbed out)."""

    def __init__(self, modules: dict[str, types.ModuleType]):
        self._modules = modules
        self._real = RealLoader()

    def get_modules(self, files_or_directories, raise_exception=False):
        return self._modules

    def get_parameters(self, module_or_path):
        return self._real.get_parameters(module_or_path)


def _make_config_module() -> types.ModuleType:
    module = types.ModuleType("broadcasting")
    module.DEFAULT = "memory"
    module.WEBSOCKET = dict(WS_SETTINGS)
    return module


def _make_app_module() -> types.ModuleType:
    # Configuration.load raises unless an "app" section exists.
    module = types.ModuleType("app")
    module.NAME = "test"
    return module


class _StubApp:
    def __init__(self, bindings: dict):
        self._bindings = bindings

    def make(self, key: str):
        return self._bindings[key]


@pytest.fixture()
def loaded_config(monkeypatch):
    """A real Configuration, loaded through the real load() loop."""
    saved_instance = Configuration._instance
    Configuration._instance = None
    try:
        stub_loader = _StubLoader(
            {"app": _make_app_module(), "broadcasting": _make_config_module()}
        )
        bindings: dict = {"loader": stub_loader, "config.location": "config"}
        app = _StubApp(bindings)
        cfg = Configuration(application=app)
        bindings["config"] = cfg
        # Facades (Config, Loader) resolve through builtins.app().
        monkeypatch.setattr(builtins, "app", lambda: app, raising=False)
        cfg.load()
        yield cfg
    finally:
        Configuration._instance = saved_instance


def test_load_materialises_websocket_dict_under_lowercase_path(loaded_config):
    assert loaded_config.get("broadcasting.websocket") == WS_SETTINGS
    # The uppercase path must NOT resolve — readers that use it get the
    # default forever, which is precisely the dead-config bug.
    assert loaded_config.get("broadcasting.WEBSOCKET") is None
    # Nested dotted access into the dict works too (Authenticate uses it).
    assert loaded_config.get("broadcasting.websocket.allowed_origins") == [
        "https://app.example"
    ]


def test_socket_max_subscriptions_reads_lowercase_key(loaded_config):
    from cara.websocket.Socket import Socket

    async def _noop(_message=None):
        return {"type": "websocket.receive"}

    sock = Socket(
        application=None, scope={"type": "websocket"}, receive=_noop, send=_noop
    )
    assert sock._max_subscriptions() == 7, (
        "Socket._max_subscriptions must honour "
        "broadcasting.websocket.max_subscriptions_per_connection — falling "
        "back to the hardcoded 25 means the config path regressed"
    )


def test_ws_authenticate_origin_allowlist_reads_lowercase_key(loaded_config):
    from cara.middleware.ws.Authenticate import Authenticate

    mw = Authenticate.__new__(Authenticate)  # skip auth-guard resolution

    def _socket_with_origin(origin: str | None):
        headers = [] if origin is None else [(b"origin", origin.encode())]
        return types.SimpleNamespace(scope={"headers": headers})

    assert mw._origin_is_allowed(_socket_with_origin("https://app.example")) is True
    assert mw._origin_is_allowed(_socket_with_origin("https://evil.example")) is False, (
        "configured broadcasting.websocket.allowed_origins was ignored — "
        "the origin allow-list can never activate if the reader misses the key"
    )
    # Non-browser clients (no Origin header) always pass.
    assert mw._origin_is_allowed(_socket_with_origin(None)) is True


def test_no_cara_source_reads_the_uppercase_websocket_path():
    offenders = []
    for path in CARA_SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="replace")
        if "broadcasting.WEBSOCKET" in text or '"WEBSOCKET"' in text:
            offenders.append(str(path.relative_to(CARA_SRC)))
    assert not offenders, (
        "uppercase WEBSOCKET config lookups found (Configuration.load "
        f"materialises lowercase keys — these can never resolve): {offenders}"
    )
