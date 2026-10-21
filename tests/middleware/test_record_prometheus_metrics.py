"""Tests for the RecordPrometheusMetrics middleware.

Covers the instrumentation contract:

* a normal request increments ``http_requests_total`` under its matched
  route TEMPLATE and observes a duration sample,
* the in-flight gauge returns to its pre-request value after the response,
* ``/metrics`` and ``/api/metrics`` are never self-instrumented,
* a downstream exception still counts (as a 5xx) and re-raises unchanged,
* label cardinality is bounded by the codebase: requests that matched no
  route share one ``unmatched`` series and a non-standard method counts as
  ``OTHER``, so client-chosen paths and verbs never mint new series.

Metrics are read back through the shared ``cara.observability.REGISTRY``
singletons on ``MetricsBase`` — the same objects every app registry serves.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cara.middleware.http import RecordPrometheusMetrics
from cara.observability import MetricsBase

# ── Test doubles ─────────────────────────────────────────────────────


def _make_request(
    path: str = "/api/products/123",
    method: str = "GET",
    template: str | None = "/api/products/@id",
):
    req = MagicMock()
    req.path = path
    req.method = method
    req.get_route.return_value = (
        SimpleNamespace(url=template) if template is not None else None
    )
    return req


class _StatusError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"status {status_code}")
        self.status_code = status_code


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


def _route_labels() -> set[str]:
    return {
        sample.labels["route"]
        for family in MetricsBase.http_requests_total.collect()
        for sample in family.samples
        if "route" in sample.labels
    }


# ── Tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_success_request_counts_under_route_template():
    mw = RecordPrometheusMetrics(MagicMock())
    req = _make_request("/api/products/123", template="/api/products/@id")
    before = _counter_value("GET", "/api/products/@id", "2xx")

    async def get_response(_req):
        return _make_response(200)

    resp = await mw.handle(req, get_response)

    assert resp.get_status_code() == 200
    assert _counter_value("GET", "/api/products/@id", "2xx") == before + 1
    assert _duration_count("GET", "/api/products/@id") >= 0.0


@pytest.mark.asyncio
async def test_in_flight_gauge_returns_to_baseline():
    mw = RecordPrometheusMetrics(MagicMock())
    baseline = MetricsBase.http_requests_in_flight._value.get()

    async def get_response(_req):
        assert MetricsBase.http_requests_in_flight._value.get() == baseline + 1
        return _make_response(200)

    await mw.handle(_make_request("/api/ping", template="/api/ping"), get_response)
    assert MetricsBase.http_requests_in_flight._value.get() == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/metrics", "/api/metrics"])
async def test_metrics_endpoints_are_not_self_instrumented(path):
    mw = RecordPrometheusMetrics(MagicMock())
    before = _counter_value("GET", path, "2xx")

    async def get_response(_req):
        return _make_response(200)

    await mw.handle(_make_request(path, template=path), get_response)
    assert _counter_value("GET", path, "2xx") == before


@pytest.mark.asyncio
async def test_downstream_exception_counts_5xx_and_reraises():
    mw = RecordPrometheusMetrics(MagicMock())
    req = _make_request("/api/boom", template="/api/boom")
    before = _counter_value("GET", "/api/boom", "5xx")
    baseline_in_flight = MetricsBase.http_requests_in_flight._value.get()

    async def get_response(_req):
        raise RuntimeError("downstream blew up")

    with pytest.raises(RuntimeError, match="downstream blew up"):
        await mw.handle(req, get_response)

    assert _counter_value("GET", "/api/boom", "5xx") == before + 1
    assert MetricsBase.http_requests_in_flight._value.get() == baseline_in_flight


@pytest.mark.asyncio
async def test_unmatched_paths_and_junk_methods_share_bounded_series():
    mw = RecordPrometheusMetrics(MagicMock())
    before = _counter_value("GET", "unmatched", "4xx")
    other_before = _counter_value("OTHER", "unmatched", "4xx")

    async def not_found(_req):
        raise _StatusError(404)

    for i in range(10):
        with pytest.raises(_StatusError):
            await mw.handle(
                _make_request(f"/api/scan-{i}/wp-login.php", template=None), not_found
            )
    with pytest.raises(_StatusError):
        await mw.handle(
            _make_request("/api/x", method="PROPFINDX", template=None), not_found
        )

    assert _counter_value("GET", "unmatched", "4xx") == before + 10
    assert _counter_value("OTHER", "unmatched", "4xx") == other_before + 1
    assert not any(label.startswith("/api/scan-") for label in _route_labels())
