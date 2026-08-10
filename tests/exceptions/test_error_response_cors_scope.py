"""The error path must not be more permissive than the success path.

``DefaultExceptionHandler._cors_headers_for_scope`` RESTATED the CORS
policy instead of reading it, and the copy had drifted OPEN:

1. It never consulted ``cors.cors.paths``. With the shipped default
   ``["api/*"]`` a request to ``/admin/...`` or ``/internal/metrics`` that
   SUCCEEDED got no ``Access-Control-Allow-Origin``, but the same route's
   401/403/404/500 came back with ``Access-Control-Allow-Origin: *``. An
   attacker's page could ``fetch()`` those deliberately-non-CORS endpoints
   cross-origin and read status and body — a working existence-and-state
   oracle over exactly the routes the operator excluded on purpose.
2. Any failure reading configuration fell back to a wildcard ACAO on every
   error response. The case where we know least about the policy granted
   the most, the inverse of §9's fail-closed rule.

These tests feed the handler an out-of-scope path and a broken config and
assert it emits NOTHING. Both would have passed a wildcard before.
"""

from __future__ import annotations

import pytest

from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.middleware.http import Cors


@pytest.fixture
def policy(monkeypatch: pytest.MonkeyPatch):
    """Install a CORS policy scoped to ``api/*`` with a wildcard origin."""

    stored = {
        "paths": ["api/*"],
        "allowed_methods": ["GET", "POST"],
        "allowed_origins": ["*"],
        "allowed_origins_patterns": [],
        "allowed_headers": ["Content-Type"],
        "exposed_headers": [],
        "max_age": 600,
        "supports_credentials": False,
    }
    monkeypatch.setattr(Cors, "load_cors_policy", lambda: dict(stored))
    return stored


def _scope(path: str, origin: bytes = b"https://evil.example") -> dict:
    return {"path": path, "headers": [(b"origin", origin)]}


def _header_names(headers: list) -> set[bytes]:
    return {name for name, _ in headers}


def test_an_out_of_scope_path_gets_no_cors_headers_on_an_error(policy) -> None:
    """The oracle: ``/internal/metrics`` answered 200 without ACAO and 500 with ``*``."""
    headers = DefaultExceptionHandler()._cors_headers_for_scope(
        _scope("/internal/metrics")
    )

    assert headers == [], (
        "An error response on a path outside cors.cors.paths must carry no "
        "CORS headers — the success path carries none."
    )


def test_an_in_scope_path_still_gets_its_cors_headers(policy) -> None:
    """The tightening must not silence the routes CORS is configured for."""
    headers = DefaultExceptionHandler()._cors_headers_for_scope(_scope("/api/listings"))

    assert [b"access-control-allow-origin", b"*"] in headers
    assert b"access-control-allow-methods" in _header_names(headers)


def test_the_path_gate_is_the_same_predicate_the_middleware_uses(policy) -> None:
    """Read the SSOT, never restate it (§5) — the copy could not learn."""
    assert Cors.path_in_cors_scope("/api/listings", policy["paths"]) is True
    assert Cors.path_in_cors_scope("/internal/metrics", policy["paths"]) is False
    assert Cors.path_in_cors_scope("/anything", []) is True


def test_an_unreadable_policy_emits_nothing_instead_of_a_wildcard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed (§9). The old fallback was ``allowed_origins = ["*"]``."""

    def _explode() -> dict:
        raise RuntimeError("config subsystem is down")

    monkeypatch.setattr(Cors, "load_cors_policy", _explode)

    headers = DefaultExceptionHandler()._cors_headers_for_scope(_scope("/api/listings"))

    assert headers == []


def test_an_unreadable_policy_is_observable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failing closed silently reads as "CORS randomly broke" to operators."""
    warnings: list[tuple] = []

    def _explode() -> dict:
        raise RuntimeError("config subsystem is down")

    monkeypatch.setattr(Cors, "load_cors_policy", _explode)
    monkeypatch.setattr(
        DefaultExceptionHandler,
        "_log_cors_policy_unavailable",
        staticmethod(lambda: warnings.append(("warned",))),
    )

    DefaultExceptionHandler()._cors_headers_for_scope(_scope("/api/listings"))

    assert warnings, "a policy we could not read must be logged, not swallowed"


def test_credentials_never_ride_alongside_a_reflected_origin(
    monkeypatch: pytest.MonkeyPatch, policy
) -> None:
    """Wildcard + credentials is the textbook CSRF primitive; treat it as deny."""
    policy["supports_credentials"] = True
    monkeypatch.setattr(Cors, "load_cors_policy", lambda: dict(policy))

    headers = DefaultExceptionHandler()._cors_headers_for_scope(_scope("/api/listings"))

    assert b"access-control-allow-origin" not in _header_names(headers)


def test_an_explicitly_allowed_origin_is_echoed_with_vary(
    monkeypatch: pytest.MonkeyPatch, policy
) -> None:
    policy["allowed_origins"] = ["https://app.example.com"]
    monkeypatch.setattr(Cors, "load_cors_policy", lambda: dict(policy))

    headers = DefaultExceptionHandler()._cors_headers_for_scope(
        _scope("/api/listings", origin=b"https://app.example.com")
    )

    assert [b"access-control-allow-origin", b"https://app.example.com"] in headers
    assert [b"vary", b"Origin"] in headers


def _handle_cors_module():
    """``cara.middleware.http.HandleCors`` the ATTRIBUTE is the class."""
    import sys

    import cara.middleware.http  # noqa: F401 — registers the submodule

    return sys.modules["cara.middleware.http.HandleCors"]


def test_the_middleware_reads_the_shared_policy_rather_than_its_own(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REPLACES an AST guard that compared two copies of the defaults.

    ``HandleCors._load_config`` used to restate every ``cors.cors.*`` key
    and its default, and a guard test parsed that method's source and
    compared the literals against ``Cors.CORS_DEFAULTS``. Needing a test
    to prove two declarations agree is the proof that there are two. The
    middleware now CALLS ``load_cors_policy``, so the property to pin is
    delegation, not equality — and delegation is observable: install a
    policy on the shared module's own seam and the middleware must be
    holding it.
    """
    from unittest.mock import MagicMock

    installed = {
        "paths": ["public/*"],
        "allowed_methods": ["PATCH"],
        "allowed_origins": ["https://sentinel.example"],
        "allowed_origins_patterns": [],
        "allowed_headers": ["X-Sentinel"],
        "exposed_headers": [],
        "max_age": 4242,
        "supports_credentials": False,
    }

    import cara.configuration as configuration

    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: installed.get(key.removeprefix("cors.cors."), default),
    )

    middleware = _handle_cors_module().HandleCors(MagicMock())

    assert middleware.config == installed, (
        "HandleCors must read cara.middleware.http.Cors.load_cors_policy; a "
        "middleware holding its own config block would be unaffected by a "
        "policy installed at the shared module's seam."
    )


def test_the_key_list_has_exactly_one_declaration() -> None:
    """The AST guard's real subject: no second list of ``cors.cors.*`` keys.

    Prose may still name a key while explaining the incident; executable
    code may not, so this reads the tree and skips docstrings.
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(_handle_cors_module()))

    docstrings = {
        node.body[0].value
        for node in ast.walk(tree)
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        )
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }

    offenders = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith("cors.cors.")
        and node not in docstrings
    ]

    assert not offenders, (
        f"cara/middleware/http/HandleCors.py names cors.cors.* keys again "
        f"({offenders}). CORS_DEFAULTS in cara/middleware/http/Cors.py is the "
        f"only place that list is allowed to exist (§5)."
    )


def test_the_early_reject_path_is_scoped_like_the_success_path(
    monkeypatch: pytest.MonkeyPatch, policy
) -> None:
    """``apply_cors_headers_to_response`` had no ``paths`` gate at all.

    ``EnforceBodySizeLimit`` and ``FilterBlockedUserAgents`` sit ahead of
    ``HandleCors`` in the chain and reject by RETURNING a response, so
    ``HandleCors`` never runs and this helper stamps the headers instead.
    Without the gate, ``/internal/metrics`` answered its 413/403 with
    ``Access-Control-Allow-Origin: *`` while the same route's 200 carried
    no ACAO — an attacker's page reads a deliberately-non-CORS endpoint
    cross-origin just by sending an oversized body or a blocked UA.
    """
    from unittest.mock import MagicMock

    import cara.configuration as configuration

    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: policy.get(key.removeprefix("cors.cors."), default),
    )

    from cara.middleware.http.HandleCors import apply_cors_headers_to_response

    class _Response:
        def __init__(self) -> None:
            self.headers: dict = {}

        def header(self, key, value):
            self.headers[key] = value
            return self

    def _request(path: str):
        request = MagicMock()
        request.path = path
        request.header = MagicMock(return_value="https://evil.example")
        return request

    out_of_scope = _Response()
    apply_cors_headers_to_response(
        MagicMock(), _request("/internal/metrics"), out_of_scope
    )

    assert out_of_scope.headers == {}, (
        "an early rejection on a path outside cors.cors.paths must carry no "
        "CORS headers — the success path carries none"
    )

    in_scope = _Response()
    apply_cors_headers_to_response(MagicMock(), _request("/api/listings"), in_scope)

    assert in_scope.headers.get("Access-Control-Allow-Origin") == "*", (
        "the gate must not silence the routes CORS is configured for"
    )


def test_the_path_gate_and_the_origin_rule_are_the_shared_ones() -> None:
    """§5: if a predicate exists twice, delete one."""
    module = _handle_cors_module()

    assert module.path_in_cors_scope is Cors.path_in_cors_scope
    assert module.resolve_allow_origin is Cors.resolve_allow_origin
    assert module.load_cors_policy is Cors.load_cors_policy
    assert not hasattr(module.HandleCors, "_is_origin_allowed")
    assert not hasattr(module.HandleCors, "_is_origin_explicitly_allowed")
