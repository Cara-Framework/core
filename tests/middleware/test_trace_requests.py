import importlib
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from cara.middleware.http.TraceRequests import TraceRequests

trace_requests_module = importlib.import_module(
    "cara.middleware.http.TraceRequests"
)


class _Response:
    def __init__(self, status=200):
        self.status = status
        self.headers = {}

    def get_status_code(self):
        return self.status

    def header(self, key, value):
        self.headers[key] = value


@pytest.mark.asyncio
async def test_request_span_extracts_parent_and_exposes_trace_id(monkeypatch):
    observed = {}

    @contextmanager
    def extracted(carrier):
        observed["carrier"] = carrier
        yield

    @contextmanager
    def span(name, *, attributes, kind):
        observed["span"] = (name, attributes, kind)
        yield

    monkeypatch.setattr(trace_requests_module.Trace, "extracted_context", extracted)
    monkeypatch.setattr(trace_requests_module.Trace, "span", span)
    monkeypatch.setattr(
        trace_requests_module.Trace,
        "set_attributes",
        lambda **attributes: observed.setdefault("status", attributes),
    )
    monkeypatch.setattr(
        trace_requests_module.Trace,
        "current_trace_id",
        lambda: "a" * 32,
    )
    request = SimpleNamespace(
        method="GET",
        path="/api/orders/123",
        headers={"traceparent": "00-" + "b" * 32 + "-" + "c" * 16 + "-01"},
    )

    response = await TraceRequests(None).handle(
        request,
        lambda _request: _return(_Response(201)),
    )

    assert observed["carrier"]["traceparent"].startswith("00-")
    assert observed["span"] == (
        "GET /api/orders/{id}",
        {
            "http.request.method": "GET",
            "http.route": "/api/orders/{id}",
        },
        "SERVER",
    )
    assert observed["status"] == {"http.response.status_code": 201}
    assert response.headers["X-Trace-ID"] == "a" * 32


@pytest.mark.asyncio
async def test_request_span_records_downstream_exception(monkeypatch):
    recorded = []
    monkeypatch.setattr(
        trace_requests_module.Trace,
        "record_exception",
        recorded.append,
    )
    request = SimpleNamespace(method="POST", path="/api/orders", headers={})
    failure = RuntimeError("boom")

    async def fail(_request):
        raise failure

    with pytest.raises(RuntimeError, match="boom"):
        await TraceRequests(None).handle(request, fail)

    assert recorded == [failure]


async def _return(value):
    return value
