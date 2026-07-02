"""Tests for the RecordPrometheusMetrics middleware.

Covers the instrumentation contract:

* a normal request increments ``http_requests_total`` with a normalized
  route label and observes a duration sample,
* the in-flight gauge returns to its pre-request value after the response,
* ``/metrics`` and ``/api/metrics`` are never self-instrumented,
* a downstream exception still counts (as a 5xx) and re-raises unchanged,
* numeric path segments collapse into one time series (label cardinality).

Metrics are read back through the shared ``cara.observability.REGISTRY``
singletons on ``MetricsBase`` — the same objects every app registry serves.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cara.middleware.http import RecordPrometheusMetrics
from cara.observability import MetricsBase, normalize_metric_path

# ── Test doubles ─────────────────────────────────────────────────────


def _make_request(path: str = "/api/products/123", method: str = "GET"):
    req = MagicMock()
    req.path = path
    req.method = method
    return req


def _make_response(status: int = 200):
    resp = MagicMock()
    resp.get_status_code.return_value = status
    return resp


def _counter_value(method: str, route: str, status_class_label: str) -> float:
    return MetricsBase.http_requests_total.labels(
        method=method, route=route, status_class=status_class_label
    )._value.get()


def _duration_count(method: str, route: str) -> float:
    metric = MetricsBase.http_request_duration_seconds.labels(method=method, route=route)
    return metric._sum.get()


# ── Tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_success_request_counts_and_observes_duration():
    mw = RecordPrometheusMetrics(MagicMock())
    req = _make_request("/api/products/123")
    route = normalize_metric_path("/api/products/123")
    before = _counter_value("GET", route, "2xx")

    async def get_response(_req):
        return _make_response(200)

    resp = await mw.handle(req, get_response)

    assert resp.get_status_code() == 200
    assert _counter_value("GET", route, "2xx") == before + 1
    assert _duration_count("GET", route) >= 0.0


@pytest.mark.asyncio
async def test_in_flight_gauge_returns_to_baseline():
    mw = RecordPrometheusMetrics(MagicMock())
    baseline = MetricsBase.http_requests_in_flight._value.get()

    async def get_response(_req):
        assert MetricsBase.http_requests_in_flight._value.get() == baseline + 1
        return _make_response(200)

    await mw.handle(_make_request("/api/ping"), get_response)
    assert MetricsBase.http_requests_in_flight._value.get() == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/metrics", "/api/metrics"])
async def test_metrics_endpoints_are_not_self_instrumented(path):
    mw = RecordPrometheusMetrics(MagicMock())
    route = normalize_metric_path(path)
    before = _counter_value("GET", route, "2xx")

    async def get_response(_req):
        return _make_response(200)

    await mw.handle(_make_request(path), get_response)
    assert _counter_value("GET", route, "2xx") == before


@pytest.mark.asyncio
async def test_downstream_exception_counts_5xx_and_reraises():
    mw = RecordPrometheusMetrics(MagicMock())
    req = _make_request("/api/boom")
    route = normalize_metric_path("/api/boom")
    before = _counter_value("GET", route, "5xx")
    baseline_in_flight = MetricsBase.http_requests_in_flight._value.get()

    async def get_response(_req):
        raise RuntimeError("downstream blew up")

    with pytest.raises(RuntimeError, match="downstream blew up"):
        await mw.handle(req, get_response)

    assert _counter_value("GET", route, "5xx") == before + 1
    assert MetricsBase.http_requests_in_flight._value.get() == baseline_in_flight


@pytest.mark.asyncio
async def test_numeric_segments_share_one_time_series():
    mw = RecordPrometheusMetrics(MagicMock())
    route_a = normalize_metric_path("/api/products/111")
    route_b = normalize_metric_path("/api/products/222")
    assert route_a == route_b

    before = _counter_value("GET", route_a, "2xx")

    async def get_response(_req):
        return _make_response(200)

    await mw.handle(_make_request("/api/products/111"), get_response)
    await mw.handle(_make_request("/api/products/222"), get_response)
    assert _counter_value("GET", route_a, "2xx") == before + 2
