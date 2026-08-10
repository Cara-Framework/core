"""Route-table reading and document assembly.

The generated route shards are read as SOURCE, never imported — importing them
boots providers, which a contract generator must not need. The aggregator's own
import list decides which shards are live, so a shard left on disk after it
stopped being imported cannot smuggle routes into the published contract.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi import (
    ControllerContract,
    ControllerResponse,
    EnvelopeNames,
    SpecInfo,
    build_spec,
    openapi_path,
    parse_routes,
    path_params,
    render,
    route_shard_paths,
)

_AGGREGATOR = """
from cara.routing import Route

from .generated.api.group_000 import route_groups as _g0

_ROUTE_GROUP_LOADERS = (_g0,)


def register_routes():
    groups = [group for loader in _ROUTE_GROUP_LOADERS for group in loader()]
    return Route.prefix("/api").routes(*groups)
"""

_SHARD = """
from cara.routing import Route


def route_groups():
    return (
        Route.prefix("/things").middleware(["auth"]).routes(
            Route.get("/", "ThingController@index", name="things.index"),
            Route.get("/@thing_id", "ThingController@show", name="things.show"),
            Route.prefix("/@thing_id/notes").routes(
                Route.post(
                    "/",
                    "NoteController@store",
                    middleware=["sudo"],
                    name="notes.store",
                ),
            ),
        ),
    )
"""


def _routes_tree(tmp_path: Path, aggregator: str = _AGGREGATOR) -> Path:
    root = tmp_path / "deployable"
    (root / "routes" / "generated" / "api").mkdir(parents=True)
    (root / "routes" / "api.py").write_text(aggregator, encoding="utf-8")
    (root / "routes" / "generated" / "api" / "group_000.py").write_text(
        _SHARD, encoding="utf-8"
    )
    return root


class TestShardDiscovery:
    def test_only_the_shards_the_aggregator_imports_are_live(self, tmp_path: Path):
        root = _routes_tree(tmp_path)
        (root / "routes" / "generated" / "api" / "group_001.py").write_text(
            _SHARD, encoding="utf-8"
        )

        # group_001 exists on disk but nothing imports it: it is not the route
        # table, so it must not contribute routes to the contract.
        assert [p.name for p in route_shard_paths(root)] == ["group_000.py"]

    def test_an_imported_shard_that_vanished_is_an_error(self, tmp_path: Path):
        root = _routes_tree(tmp_path)
        (root / "routes" / "generated" / "api" / "group_000.py").unlink()

        with pytest.raises(FileNotFoundError):
            route_shard_paths(root)

    def test_an_aggregator_with_no_shards_is_an_error(self, tmp_path: Path):
        root = _routes_tree(
            tmp_path, aggregator="def register_routes():\n    return ()\n"
        )

        with pytest.raises(RuntimeError):
            route_shard_paths(root)


class TestRouteParsing:
    def test_prefixes_accumulate_down_the_group_tree(self, tmp_path: Path):
        root = _routes_tree(tmp_path)
        routes = parse_routes(route_shard_paths(root), base_prefix="/api")

        assert [(r["method"], r["path"]) for r in routes] == [
            ("GET", "/api/things/"),
            ("GET", "/api/things/@thing_id"),
            ("POST", "/api/things/@thing_id/notes/"),
        ]
        assert routes[0]["controller"] == "ThingController"
        assert routes[0]["name"] == "things.index"
        assert routes[0]["middleware"] == ["auth"]
        assert routes[2]["middleware"] == ["auth", "sudo"]

    def test_route_holes_become_openapi_parameters(self):
        assert openapi_path("/api/things/@thing_id/notes/@note_id:int") == (
            "/api/things/{thing_id}/notes/{note_id}"
        )
        assert [p["name"] for p in path_params("/a/{x}/b/{y}")] == ["x", "y"]
        assert path_params("/a/b") == []


def _spec(**overrides):
    kwargs = dict(
        info=SpecInfo(title="T", description="D"),
        schemas={"RowResource": {"type": "object", "properties": {}}},
        mapping={"ThingController@index": ("RowResource", True)},
        routes=[
            {
                "method": "GET",
                "path": "/api/things/",
                "controller": "ThingController",
                "action": "index",
                "name": "things.index",
            },
            {
                "method": "GET",
                "path": "/api/things/@thing_id",
                "controller": "ThingController",
                "action": "show",
                "name": "",
            },
        ],
        envelope_components={"_Meta": {"type": "object"}, "ApiErrorBody": {}},
    )
    kwargs.update(overrides)
    return build_spec(**kwargs)


class TestSpecAssembly:
    def test_a_mapped_action_gets_its_resource_as_the_data_payload(self):
        spec = _spec()
        body = spec["paths"]["/api/things/"]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]

        assert body["properties"]["data"] == {
            "type": "array",
            "items": {"$ref": "#/components/schemas/RowResource"},
        }
        assert body["properties"]["meta"] == {"$ref": "#/components/schemas/_Meta"}
        assert spec["x-generation"]["routes_with_response_schema"] == 1

    def test_an_unmapped_action_still_documents_the_envelope(self):
        spec = _spec()
        operation = spec["paths"]["/api/things/{thing_id}"]["get"]

        # Honest: no resource maps here, so the payload is permissive rather
        # than absent — the envelope itself is still a contract.
        body = operation["responses"]["200"]["content"]["application/json"]["schema"]
        assert body["properties"]["data"] == {}
        assert operation["operationId"] == "ThingController@show"
        assert [p["name"] for p in operation["parameters"]] == ["thing_id"]

    def test_a_cursor_action_gains_page_parameters_and_the_cursor_meta(self):
        spec = _spec(
            cursor_actions={"ThingController@index"},
            envelope_components={
                "_Meta": {"type": "object"},
                "_CursorMeta": {"type": "object"},
                "ApiErrorBody": {},
            },
        )
        operation = spec["paths"]["/api/things/"]["get"]
        body = operation["responses"]["200"]["content"]["application/json"]["schema"]

        assert [p["name"] for p in operation["parameters"]] == ["limit", "cursor"]
        assert body["required"] == ["data", "meta"]
        assert body["properties"]["meta"] == {"$ref": "#/components/schemas/_CursorMeta"}

    def test_envelope_component_names_are_the_application_s_to_choose(self):
        spec = _spec(
            envelope_components={"Meta": {"type": "object"}, "Error": {}},
            envelope=EnvelopeNames(meta="Meta", error="Error"),
        )
        operation = spec["paths"]["/api/things/"]["get"]

        assert operation["responses"]["422"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/Error"
        }

    def test_request_security_and_observed_status_are_operation_specific(self):
        spec = _spec(
            routes=[
                {
                    "method": "POST",
                    "path": "/api/things/",
                    "controller": "ThingController",
                    "action": "store",
                    "name": "things.store",
                    "middleware": ["auth", "tenant", "throttle:api"],
                }
            ],
            request_schemas={
                "StoreRequest": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}},
                }
            },
            controller_contracts={
                "ThingController@store": ControllerContract(
                    requests=("StoreRequest",),
                    responses=(ControllerResponse(201, "envelope"),),
                )
            },
            security_schemes={"BearerAuth": {"type": "http", "scheme": "bearer"}},
            middleware_security={"auth": [{"BearerAuth": []}]},
            middleware_error_statuses={
                "auth": (401,),
                "tenant": (403,),
                "throttle:*": (429,),
            },
        )

        operation = spec["paths"]["/api/things/"]["post"]
        assert operation["security"] == [{"BearerAuth": []}]
        assert operation["requestBody"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/StoreRequest"
        }
        assert set(operation["responses"]) == {"201", "401", "403", "422", "429"}
        assert spec["components"]["securitySchemes"]["BearerAuth"]["scheme"] == ("bearer")

    def test_every_ref_resolves_to_a_declared_component(self):
        spec = _spec()
        declared = set(spec["components"]["schemas"])

        def refs(node):
            if isinstance(node, dict):
                if "$ref" in node:
                    yield node["$ref"].rsplit("/", 1)[-1]
                for value in node.values():
                    yield from refs(value)
            elif isinstance(node, list):
                for value in node:
                    yield from refs(value)

        assert set(refs(spec["paths"])) <= declared

    def test_the_document_renders_deterministically(self):
        assert render(_spec()) == render(_spec())
        assert render(_spec()).endswith("\n")
