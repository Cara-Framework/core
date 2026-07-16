"""Route-label normalization — the cardinality guard for HTTP metrics.

``normalize_metric_path`` collapses dynamic URL segments into placeholders so
``{namespace}_http_requests_total{route=...}`` (emitted by
``RecordPrometheusMetrics``) carries a BOUNDED label set. The regression that
motivated the prefixed-public-id rule: ``MakesPublicId`` ids look like
``CHN01KX…`` — not all-digit, not a UUID, and not exactly 26 alnum chars — so
the older heuristics left them intact and every channel/order/case id spawned
its own time-series.
"""

from __future__ import annotations

import ulid

from cara.observability import normalize_metric_path


def test_numeric_segment_collapses_to_id():
    assert normalize_metric_path("/api/products/123") == "/api/products/{id}"


def test_uuid_segment_collapses():
    uid = "550e8400-e29b-41d4-a716-446655440000"
    assert normalize_metric_path(f"/api/x/{uid}") == "/api/x/{uuid}"


def test_bare_ulid_segment_collapses():
    u = str(ulid.new())
    assert normalize_metric_path(f"/api/x/{u}") == "/api/x/{ulid}"


def test_prefixed_public_id_collapses_to_id():
    # The real-world footgun: ``PREFIX + ULID`` (no separator). Any prefix
    # this codebase uses must fold to a single ``{id}`` label.
    for prefix in ("CHN", "PRD", "ORD", "STK", "SYN", "TEAM", "FFLG"):
        pid = f"{prefix}{ulid.new()}"
        assert normalize_metric_path(f"/api/x/{pid}") == "/api/x/{id}"


def test_two_distinct_public_ids_share_one_route_label():
    a = normalize_metric_path(f"/channels/CHN{ulid.new()}/pull")
    b = normalize_metric_path(f"/channels/CHN{ulid.new()}/pull")
    assert a == b == "/channels/{id}/pull"


def test_static_words_are_not_collapsed():
    # Bounded, low-cardinality segments must survive verbatim — including the
    # lowercase route verbs and the short bounded marketplace/app keys.
    for path in (
        "/api/channels/pull",
        "/api/oauth/amazon/callback",
        "/api/apps/ebay/install",
        "/api/recovery/cases/dismiss",
        "/metrics",
    ):
        assert normalize_metric_path(path) == path


def test_root_and_empty_segments_preserved():
    assert normalize_metric_path("/") == "/"
    assert normalize_metric_path("") == ""
