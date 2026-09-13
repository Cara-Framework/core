"""A middleware's response headers are documented where it sends them, and only there."""

from __future__ import annotations

import pytest

from cara.openapi import (
    RATE_LIMIT_HEADERS,
    ControllerContract,
    ControllerResponse,
    InvalidHeaderComponent,
    MiddlewareHeaders,
    SpecInfo,
    build_spec,
)


def _spec(**overrides):
    kwargs = dict(
        info=SpecInfo(title="T", description="D"),
        schemas={"RowResource": {"type": "object", "properties": {}}},
        mapping={"ThingController@store": ("RowResource", False)},
        routes=[
            {
                "method": "POST",
                "path": "/api/things/",
                "controller": "ThingController",
                "action": "store",
                "name": "things.store",
                "middleware": ["auth", "throttle:api"],
            },
            {
                "method": "GET",
                "path": "/api/moved",
                "controller": "MovedController",
                "action": "go",
                "name": "moved.go",
                "middleware": ["throttle:api"],
            },
            {
                "method": "GET",
                "path": "/api/open",
                "controller": "OpenController",
                "action": "show",
                "name": "open.show",
                "middleware": ["auth"],
            },
        ],
        envelope_components={"_Meta": {"type": "object"}, "ApiErrorBody": {}},
        controller_contracts={
            "ThingController@store": ControllerContract(
                requests=(), responses=(ControllerResponse(201, "envelope"),)
            ),
            "MovedController@go": ControllerContract(
                requests=(), responses=(ControllerResponse(302, "redirect"),)
            ),
        },
        middleware_error_statuses={"auth": (401,), "throttle:*": (429,)},
        middleware_headers={"throttle:*": RATE_LIMIT_HEADERS},
    )
    kwargs.update(overrides)
    return build_spec(**kwargs)


def _ref(header: str) -> dict:
    return {"$ref": f"#/components/headers/{header}"}


def _header_refs(node):
    if isinstance(node, dict):
        ref = node.get("$ref", "")
        if ref.startswith("#/components/headers/"):
            yield ref.rsplit("/", 1)[-1]
        for value in node.values():
            yield from _header_refs(value)
    elif isinstance(node, list):
        for value in node:
            yield from _header_refs(value)


class TestRateLimitHeaders:
    def test_what_the_operation_returns_carries_the_quota_pair(self):
        created = _spec()["paths"]["/api/things/"]["post"]["responses"]["201"]

        assert created["headers"] == {
            "RateLimit-Policy": _ref("RateLimit-Policy"),
            "RateLimit": _ref("RateLimit"),
        }

    def test_the_refusal_also_says_when_to_come_back(self):
        refused = _spec()["paths"]["/api/things/"]["post"]["responses"]["429"]

        assert list(refused["headers"]) == [
            "Retry-After",
            "RateLimit-Policy",
            "RateLimit",
        ]
        assert refused["x-http-status"] == 429

    def test_an_error_raised_before_or_past_the_throttle_carries_none(self):
        responses = _spec()["paths"]["/api/things/"]["post"]["responses"]

        assert "headers" not in responses["401"]
        assert "headers" not in responses["422"]

    def test_an_unthrottled_operation_carries_none(self):
        responses = _spec()["paths"]["/api/open"]["get"]["responses"]

        assert all("headers" not in response for response in responses.values())

    def test_a_header_the_response_already_documents_stays_first(self):
        moved = _spec()["paths"]["/api/moved"]["get"]["responses"]["302"]

        assert list(moved["headers"]) == ["Location", "RateLimit-Policy", "RateLimit"]

    def test_every_header_ref_resolves_and_nothing_unreferenced_is_published(self):
        spec = _spec()
        published = spec["components"]["headers"]

        assert set(_header_refs(spec["paths"])) == set(published)
        assert published["Retry-After"]["schema"] == {"type": "integer", "minimum": 0}

        unrefused = _spec(middleware_error_statuses={"auth": (401,)})
        assert set(unrefused["components"]["headers"]) == {
            "RateLimit-Policy",
            "RateLimit",
        }

    def test_no_declaration_publishes_no_header_components(self):
        assert "headers" not in _spec(middleware_headers=None)["components"]


class TestDeclarationErrors:
    def test_naming_a_header_it_does_not_define_is_refused(self):
        with pytest.raises(InvalidHeaderComponent):
            MiddlewareHeaders(components={}, refused={429: ("Retry-After",)})

    def test_two_declarations_defining_one_header_differently_are_refused(self):
        other = MiddlewareHeaders(
            components={"RateLimit": {"schema": {"type": "integer"}}},
            passed=("RateLimit",),
        )

        with pytest.raises(InvalidHeaderComponent):
            _spec(middleware_headers={"throttle:*": RATE_LIMIT_HEADERS, "auth": other})
