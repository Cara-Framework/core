"""The framework ships no metrics port number.

A port assignment is a deployment decision owned by each product's own
``config/metrics.py``. Cara used to fall back to a hard-coded ``9101`` — which
is one specific product's production worker-metrics port. A framework default
like that hands that product's socket number to every other product built on
cara, and a role that never configured a port looks healthy while listening on
somebody else's number.

So: no fallback. An unset ``metrics.port`` runs without ``/metrics`` and says
so loudly; an explicit ``0`` stays the silent, documented opt-out.
"""

from __future__ import annotations

import pytest

from cara.observability import Metrics


@pytest.fixture(autouse=True)
def _reset_server_state(monkeypatch):
    monkeypatch.setattr(Metrics, "_http_server_started", False, raising=False)


@pytest.fixture
def _never_binds(monkeypatch):
    """Fail the test if a socket is opened — none of these cases should."""

    def _boom(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError("start_http_server bound a socket it should not have")

    monkeypatch.setattr(Metrics, "_prom_start_http_server", _boom)


def _stub_config(monkeypatch, value):
    monkeypatch.setattr(Metrics, "config", lambda _key, default=None: value)


def test_unconfigured_port_serves_nothing(monkeypatch, _never_binds):
    _stub_config(monkeypatch, None)

    assert Metrics.start_http_server(role="queue-worker") is None
    assert Metrics._http_server_started is False


def test_unconfigured_port_warns_with_the_role(monkeypatch, _never_binds):
    _stub_config(monkeypatch, None)
    warnings: list[str] = []

    from cara.facades import Log

    monkeypatch.setattr(Log, "warning", lambda msg, *a, **k: warnings.append(str(msg)))

    Metrics.start_http_server(role="queue-relay")

    assert len(warnings) == 1
    assert "metrics.port" in warnings[0]
    assert "queue-relay" in warnings[0]


def test_no_product_port_number_leaks_as_a_default(monkeypatch, _never_binds):
    # The regression this file exists for: an unset key must not resolve to
    # 9101 (or any other number the framework invented).
    _stub_config(monkeypatch, None)
    assert Metrics.start_http_server() is None


def test_explicit_zero_config_is_a_silent_opt_out(monkeypatch, _never_binds):
    _stub_config(monkeypatch, 0)
    warnings: list[str] = []

    from cara.facades import Log

    monkeypatch.setattr(Log, "warning", lambda msg, *a, **k: warnings.append(str(msg)))

    assert Metrics.start_http_server(role="queue-worker") is None
    assert warnings == []


def test_configured_port_is_used(monkeypatch):
    bound: list[int] = []
    monkeypatch.setattr(
        Metrics,
        "_prom_start_http_server",
        lambda port, addr=None, registry=None: bound.append(port),
    )
    _stub_config(monkeypatch, "9400")

    assert Metrics.start_http_server(role="queue-worker") == 9400
    assert bound == [9400]
