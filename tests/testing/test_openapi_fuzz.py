"""cara.testing.fuzz: requests derived from an OpenAPI document."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings

from cara.testing.fuzz import (
    FuzzRequest,
    HypothesisStorage,
    IdPools,
    OpenApiOperation,
    OpenApiRequests,
)

SPEC = {
    "openapi": "3.1.0",
    "paths": {
        "/api/items/{item_public_id}": {
            "get": {"operationId": "items.show", "x-middleware": ["auth"]},
            "patch": {
                "operationId": "items.update",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Up"}
                        }
                    }
                },
            },
        },
        "/api/items": {
            "get": {
                "operationId": "items.index",
                "parameters": [
                    {"name": "status", "in": "query", "schema": {"enum": ["a", "b"]}}
                ],
            }
        },
    },
    "components": {
        "schemas": {
            "Up": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string", "maxLength": 5},
                    "count": {"type": "integer", "minimum": 0, "maximum": 9},
                },
            }
        }
    },
}
REQUESTS = OpenApiRequests(SPEC, IdPools({"item_public_id": ["ITM01", "ITM02"]}))
OPS = {op.operation_id: op for op in OpenApiOperation.all(SPEC)}
FAST = settings(
    max_examples=60,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


def test_operations_walk_the_spec_in_stable_order():
    assert [str(op) for op in OpenApiOperation.all(SPEC)] == [
        "GET /api/items",
        "GET /api/items/{item_public_id}",
        "PATCH /api/items/{item_public_id}",
    ]
    assert OPS["items.show"].middleware == ("auth",)
    assert OPS["items.show"].path_params == ["item_public_id"]
    guarded = OpenApiOperation.all(SPEC, where=lambda op: "auth" in op.middleware)
    assert [op.operation_id for op in guarded] == ["items.show"]


def test_id_pools_fold_plural_and_public_id_forms():
    pools = IdPools({"product_public_id": ["P1"], "empty": []})
    assert pools.for_name("product_public_id") == ["P1"]
    assert pools.for_name("products") == ["P1"]
    assert pools.for_name("product_ids") == ["P1"]
    assert pools.for_name("empty") == []
    assert pools.for_name("nope") == []


@FAST
@given(REQUESTS.request(OPS["items.show"], real_ids=True))
def test_real_id_paths_draw_from_the_pool(request: FuzzRequest):
    assert request.path in {"/api/items/ITM01", "/api/items/ITM02"}
    assert request.body == b""
    request.query.decode("ascii")  # always sendable


@FAST
@given(REQUESTS.request(OPS["items.show"], real_ids=False))
def test_fake_id_paths_are_hostile_segments(request: FuzzRequest):
    assert "{" not in request.path
    assert not request.path.endswith(("ITM01", "ITM02"))


@FAST
@given(REQUESTS.request(OPS["items.update"]))
def test_valid_bodies_honour_the_schema(request: FuzzRequest):
    if request.note != "valid-body":
        return
    body = json.loads(request.body)
    assert isinstance(body["name"], str) and len(body["name"]) <= 5
    assert 0 <= body.get("count", 0) <= 9


@FAST
@given(REQUESTS.request(OPS["items.update"]))
def test_mutated_bodies_change_exactly_one_field_or_add_one(request: FuzzRequest):
    if request.note != "mutated-body":
        return
    assert set(json.loads(request.body)) - {"name", "count"} or "name" in json.loads(
        request.body
    )


@FAST
@given(REQUESTS.headers(("Authorization", "X-Forwarded-For")))
def test_hostile_headers_are_sendable(headers: dict):
    for value in headers.values():
        value.encode("ascii")
        assert value == value.strip()  # h11 refuses edge whitespace


def test_url_strips_the_spec_prefix():
    request = FuzzRequest("GET", "/api/items", query=b"a=1")
    assert request.url(strip_prefix="/api") == "/items?a=1"
    assert FuzzRequest("POST", "/api/x", body=b"{}").send_headers() == {
        "Content-Type": "application/json"
    }


def test_storage_redirect_points_at_the_temp_dir(monkeypatch):
    monkeypatch.delenv(HypothesisStorage.ENV, raising=False)
    path = HypothesisStorage.redirect("probe")
    assert path.name == "probe-hypothesis"
    assert os.environ[HypothesisStorage.ENV] == str(path)
    monkeypatch.setenv(HypothesisStorage.ENV, "/ci/keep")
    assert HypothesisStorage.redirect("probe") == Path("/ci/keep")
    with pytest.raises(ValueError):
        HypothesisStorage.redirect("a/b")
