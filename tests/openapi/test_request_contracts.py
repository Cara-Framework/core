"""Static request and controller contract extraction."""

from __future__ import annotations

from pathlib import Path

from cara.openapi import ControllerContractExtractor, FormRequestSchemaExtractor


def _write_source(tmp_path: Path, folder: str, source: str) -> Path:
    root = tmp_path / folder
    root.mkdir()
    (root / "Contracts.py").write_text(source, encoding="utf-8")
    return root


def test_form_request_rules_become_nested_json_schema(tmp_path: Path):
    requests = _write_source(
        tmp_path,
        "requests",
        '''
class IndexRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {
            **cursor_rules(),
            "status": "nullable|string|in:open,closed",
        }


class StoreRequest(FormRequest):
    """POST /things body."""

    def rules(self):
        return {
            "items": "required|array|between:1,20",
            "items.*": "required|dict",
            "items.*.quantity": "required|integer|between:1,100",
            "legacy_page": "missing",
        }
''',
    )

    schemas = FormRequestSchemaExtractor(requests).extract()

    index = schemas["IndexRequest"]
    assert index["x-cara-location"] == "query"
    assert set(index["properties"]) == {"limit", "cursor", "status"}
    assert "cursor" not in index.get("required", [])
    assert index["x-cara-forbidden-fields"] == ["offset", "page", "per_page"]

    store = schemas["StoreRequest"]
    items = store["properties"]["items"]
    assert items["minItems"] == 1
    assert items["maxItems"] == 20
    assert items["items"]["required"] == ["quantity"]
    assert items["items"]["properties"]["quantity"]["maximum"] == 100
    assert store["x-cara-forbidden-fields"] == ["legacy_page"]


def test_controller_contracts_capture_request_and_every_response_status(
    tmp_path: Path,
):
    controllers = _write_source(
        tmp_path,
        "controllers",
        """
from http import HTTPStatus


class ThingController:
    async def store(self, request, response):
        data = await StoreRequest().validate_request(request)
        return response.envelope(data, status=201)

    async def destroy(self, request, response):
        return response.no_content()

    async def callback(self, request, response):
        return response.redirect("/done")

    async def health(self, request, response):
        return response.json(
            {"ok": True},
            HTTPStatus.OK if request.ready else HTTPStatus.SERVICE_UNAVAILABLE,
        )
""",
    )

    contracts = ControllerContractExtractor(controllers).extract()

    store = contracts["ThingController@store"]
    assert store.requests == ("StoreRequest",)
    assert [(item.status, item.kind) for item in store.responses] == [(201, "envelope")]
    assert [
        (item.status, item.kind)
        for item in contracts["ThingController@destroy"].responses
    ] == [(204, "empty")]
    assert [
        (item.status, item.kind)
        for item in contracts["ThingController@callback"].responses
    ] == [(302, "redirect")]
    assert [
        (item.status, item.kind) for item in contracts["ThingController@health"].responses
    ] == [(200, "json"), (503, "json")]
