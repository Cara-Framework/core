from __future__ import annotations

from importlib import import_module

import pytest

Metrics = import_module("cara.observability.MetricsBase")
RuntimeMetrics = import_module("cara.observability._RuntimeMetrics")


@pytest.fixture(autouse=True)
def _reset_server_state(monkeypatch):
    monkeypatch.setattr(RuntimeMetrics, "_http_server_started", False)


@pytest.mark.parametrize("value", [True, "9400", 1.5])
def test_metrics_server_rejects_non_integer_ports(value) -> None:
    with pytest.raises((TypeError, ValueError)):
        Metrics.start_http_server(
            port=value,
            service="test-services",
            role="worker",
        )


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_port_is_the_silent_opt_out(value) -> None:
    # METRICS_PORT=0 is the documented opt-out: no server, no complaint.
    assert (
        Metrics.start_http_server(
            port=value,
            service="test-services",
            role="worker",
        )
        is None
    )
    assert RuntimeMetrics._http_server_started is False


def test_unconfigured_port_warns_and_runs_without_metrics() -> None:
    # No argument and no ``metrics.port`` config: observability must never
    # be the reason work stops — warn loudly, run without /metrics.
    assert (
        Metrics.start_http_server(
            service="test-services",
            role="worker",
        )
        is None
    )
    assert RuntimeMetrics._http_server_started is False


def test_explicit_port_and_identity_are_used(monkeypatch) -> None:
    bound: list[int] = []
    monkeypatch.setattr(
        RuntimeMetrics,
        "_prom_start_http_server",
        lambda port, addr=None, registry=None: bound.append(port),
    )

    assert (
        Metrics.start_http_server(
            port=9400,
            service="test-services",
            role="worker",
        )
        == 9400
    )
    assert bound == [9400]
