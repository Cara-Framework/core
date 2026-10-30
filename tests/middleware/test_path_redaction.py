"""``PathRedaction``: a capability carried in a URL path never reaches a log.

The route table says which path segment is a secret — every parameter named
in ``logging.secret_path_parameters`` (default ``("token",)``). These pins
cover the resolution of that list, the matching (by pattern, whatever the
method), the per-router cache and the fail-closed fallback.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cara.middleware.http import PathRedaction
from cara.routing import Route, Router

redaction_module = importlib.import_module("cara.middleware.http.PathRedaction")

REDACTED = PathRedaction.REDACTED_SEGMENT
SECRET = "ss_9Xq2-secret"


def _controller(_request=None, _response=None):
    return {"ok": True}


def _routes() -> tuple[Route, ...]:
    return (
        Route.post("/api/webhooks/shipstation/@token", _controller),
        Route.get("/api/oauth/@provider/share/@token:slug", _controller),
        Route.get("/api/files/@token/@name?", _controller),
        Route.get("/api/lists/@selection_token", _controller),
        Route.get("/api/products/@id", _controller),
    )


def _application(*routes) -> SimpleNamespace:
    router = Router(None, *routes)
    return SimpleNamespace(make=lambda _key: router)


@pytest.fixture(autouse=True)
def _unconfigured(monkeypatch):
    """Framework defaults: no product config behind ``config()``."""
    monkeypatch.setattr(redaction_module, "config", lambda _key, default=None: default)


def _configure(monkeypatch, value) -> None:
    monkeypatch.setattr(
        redaction_module,
        "config",
        lambda key, default=None: (
            value if key == "logging.secret_path_parameters" else default
        ),
    )


# ── The secret parameter list ────────────────────────────────────────


def test_the_framework_default_is_token():
    assert PathRedaction.DEFAULT_SECRET_PARAMETERS == ("token",)
    assert PathRedaction.secret_parameters() == frozenset({"token"})


def test_a_configured_list_replaces_the_default(monkeypatch):
    _configure(monkeypatch, ["signature", "share_key"])

    assert PathRedaction.secret_parameters() == frozenset({"signature", "share_key"})


def test_a_single_configured_name_is_one_parameter(monkeypatch):
    _configure(monkeypatch, "signature")

    assert PathRedaction.secret_parameters() == frozenset({"signature"})


def test_a_non_iterable_configuration_falls_back_to_the_default(monkeypatch):
    _configure(monkeypatch, 7)

    assert PathRedaction.secret_parameters() == frozenset({"token"})


# ── Which routes are secret ──────────────────────────────────────────


def test_secret_routes_are_the_routes_binding_a_secret_parameter_by_exact_name():
    """``@token:slug`` and ``@token?`` bind ``token``; ``@selection_token``
    does not — a name is matched exactly, never as a substring."""
    found = PathRedaction.secret_routes(_routes(), {"token"})

    assert [(route.url, width, positions) for route, width, positions in found] == [
        ("/api/webhooks/shipstation/@token", 4, (3,)),
        ("/api/oauth/@provider/share/@token:slug", 5, (4,)),
        ("/api/files/@token/@name?", 4, (2,)),
    ]


# ── Redaction ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "path,expected",
    [
        (f"/api/webhooks/shipstation/{SECRET}", f"/api/webhooks/shipstation/{REDACTED}"),
        ("/api/oauth/ebay/share/eyJjIjoi-c2ln", f"/api/oauth/ebay/share/{REDACTED}"),
        ("/api/files/att_tok-77/invoice.pdf", f"/api/files/{REDACTED}/invoice.pdf"),
        # A sloppy trailing or double slash still matches the pattern.
        (f"/api/webhooks/shipstation/{SECRET}/", f"/api/webhooks/shipstation/{REDACTED}"),
        (f"/api//webhooks/shipstation/{SECRET}", f"/api/webhooks/shipstation/{REDACTED}"),
    ],
)
def test_a_token_bearing_path_is_redacted(path, expected):
    routes = PathRedaction.secret_routes(_routes(), {"token"})

    assert PathRedaction.redact(path, routes) == expected


def test_an_optional_parameter_left_out_logs_the_route_pattern():
    """A missing optional segment shifts the positions; the pattern itself
    carries no value at all."""
    routes = PathRedaction.secret_routes(_routes(), {"token"})

    assert PathRedaction.redact("/api/files/att_tok-77", routes) == (
        "/api/files/@token/@name?"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/api/products/PRD01HZY3V8Q",
        "/api/lists/sel_01HZY3",
        "/api/webhooks/shipstation",
        "/api/webhooks/ebay/notifications/CHN5",
    ],
)
def test_an_ordinary_path_is_unchanged(path):
    routes = PathRedaction.secret_routes(_routes(), {"token"})

    assert PathRedaction.redact(path, routes) == path


# ── loggable_path: config, router, cache, fail-closed ────────────────


def test_loggable_path_reads_the_booted_router():
    application = _application(*_routes())

    assert PathRedaction.loggable_path(
        f"/api/webhooks/shipstation/{SECRET}", application
    ) == (f"/api/webhooks/shipstation/{REDACTED}")


def test_loggable_path_honours_the_configured_parameters(monkeypatch):
    _configure(monkeypatch, ["signature"])
    application = _application(
        Route.get("/api/exports/@signature", _controller), *_routes()
    )

    # ``signature`` is now the secret; ``token`` no longer is.
    assert PathRedaction.loggable_path("/api/exports/sig-1", application) == (
        f"/api/exports/{REDACTED}"
    )
    assert PathRedaction.loggable_path(
        f"/api/webhooks/shipstation/{SECRET}", application
    ) == (f"/api/webhooks/shipstation/{SECRET}")


def test_the_route_table_is_read_once_per_router(monkeypatch):
    calls = []
    real = PathRedaction.secret_routes

    def _counting(routes, parameters):
        calls.append(1)
        return real(routes, parameters)

    monkeypatch.setattr(PathRedaction, "secret_routes", staticmethod(_counting))
    application = _application(*_routes())

    for _ in range(3):
        PathRedaction.loggable_path(f"/api/webhooks/shipstation/{SECRET}", application)

    assert len(calls) == 1


def test_an_empty_path_is_returned_as_is():
    assert PathRedaction.loggable_path("", _application()) == ""


def test_a_failed_redaction_withholds_the_whole_path(monkeypatch):
    """Fail closed: when the router cannot be read the raw path — and the
    secret in it — is not written; the operator is told why."""
    warning = Mock()
    monkeypatch.setattr(redaction_module, "Log", SimpleNamespace(warning=warning))

    def _no_router(_key):
        raise RuntimeError("router is not bound")

    logged = PathRedaction.loggable_path(
        f"/api/webhooks/shipstation/{SECRET}", SimpleNamespace(make=_no_router)
    )

    assert logged == REDACTED
    assert "router is not bound" in warning.call_args.args[0]
    assert warning.call_args.kwargs["category"] == PathRedaction.LOG_CATEGORY
