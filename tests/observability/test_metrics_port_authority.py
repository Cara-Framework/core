from __future__ import annotations

from importlib import import_module

import pytest

Metrics = import_module("cara.observability.MetricsBase")
RuntimeMetrics = import_module("cara.observability._RuntimeMetrics")


@pytest.fixture(autouse=True)
def _reset_server_state(monkeypatch):
    monkeypatch.setattr(RuntimeMetrics, "_http_server_started", False)


@pytest.mark.parametrize("value", [None, 0, -1, True, "9400", 1.5])
def test_metrics_server_requires_an_explicit_positive_integer_port(value) -> None:
    with pytest.raises((RuntimeError, TypeError, ValueError)):
        Metrics.start_http_server(
            port=value,
            service="test-services",
            role="worker",
        )


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
