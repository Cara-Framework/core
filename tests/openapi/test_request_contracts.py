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
            "items.*.amount": "required|decimal_text:14,4",
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
    assert items["items"]["required"] == ["amount", "quantity"]
    assert items["items"]["additionalProperties"] is False
    amount = items["items"]["properties"]["amount"]
    assert amount["type"] == "string"
    assert amount["maxLength"] == 15
    assert amount["x-cara-decimal-precision"] == 14
    assert amount["x-cara-decimal-scale"] == 4
    assert items["items"]["properties"]["quantity"]["maximum"] == 100
    assert store["x-cara-forbidden-fields"] == ["legacy_page"]


def test_imported_rule_spreads_and_nested_inheritance_stay_complete(tmp_path: Path):
    requests = tmp_path / "requests"
    requests.mkdir()
    (requests / "DefinitionRules.py").write_text(
        """
AUTOMATION_DEFINITION_RULES = {
    "conditions": "required|array",
    "conditions.*": "required|dict",
    "conditions.*.field": "required|string",
    "conditions.*.value": "required|decimal_text:14,4",
}
""",
        encoding="utf-8",
    )
    (requests / "Contracts.py").write_text(
        """
from app.requests.DefinitionRules import AUTOMATION_DEFINITION_RULES


class DefinitionRequest(FormRequest):
    def rules(self):
        return {**AUTOMATION_DEFINITION_RULES, "name": "required|string"}


class UpdateRequest(DefinitionRequest):
    def rules(self):
        return {"revision": "required|integer"}
""",
        encoding="utf-8",
    )

    schemas = FormRequestSchemaExtractor(requests).extract()

    base = schemas["DefinitionRequest"]
    assert "x-cara-rules-partial" not in base
    assert (
        base["properties"]["conditions"]["items"]["properties"]["value"]["type"]
        == "string"
    )
    inherited = schemas["UpdateRequest"]
    assert set(inherited["properties"]) == {"conditions", "name", "revision"}
    assert inherited["properties"]["conditions"]["items"]["required"] == [
        "field",
        "value",
    ]


def test_named_and_wildcard_children_describe_an_open_typed_object(tmp_path: Path):
    requests = _write_source(
        tmp_path,
        "requests",
        """
class SettingsRequest(FormRequest):
    def rules(self):
        return {
            "settings": "required|dict",
            "settings.*": "nullable",
            "settings.reprice": "nullable|dict",
            "settings.reprice.min_price": "required|decimal_text:14,4",
        }


class SettingsUpdateRequest(SettingsRequest):
    def rules(self):
        return {"revision": "required|integer"}
""",
    )

    schemas = FormRequestSchemaExtractor(requests).extract()
    settings = schemas["SettingsRequest"]["properties"]["settings"]

    assert settings["type"] == "object"
    assert settings["additionalProperties"]["nullable"] is True
    assert settings["properties"]["reprice"]["additionalProperties"] is False
    assert (
        settings["properties"]["reprice"]["properties"]["min_price"]["type"] == "string"
    )
    inherited = schemas["SettingsUpdateRequest"]["properties"]["settings"]
    assert inherited["additionalProperties"]["nullable"] is True


def test_a_filter_tree_query_parameter_documents_its_grammar_and_schema(
    tmp_path: Path,
):
    """``filter_tree:<schema>`` is the framework's own grammar (``cara.filtering``),
    so the query parameter it guards says what to send: the schema it is judged
    against, the wire grammar, and the parser's length cap. It used to land in
    ``x-cara-unsupported-rules`` and leave ``filters`` a bare string, and an API
    customer had no way to learn the grammar from the published reference."""
    from cara.filtering import RAW_LENGTH_CAP
    from cara.openapi import request_query_parameters

    requests = _write_source(
        tmp_path,
        "requests",
        '''
class IndexRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {"filters": "nullable|string|filter_tree:things.index"}
''',
    )

    schema = FormRequestSchemaExtractor(requests).extract()["IndexRequest"]
    filters = schema["properties"]["filters"]
    assert "x-cara-unsupported-rules" not in filters
    assert filters["x-cara-filter-tree"] == "things.index"
    assert filters["maxLength"] == RAW_LENGTH_CAP

    (parameter,) = request_query_parameters(schema)
    assert parameter["name"] == "filters"
    assert parameter["x-filter-schema"] == "things.index"
    assert parameter["schema"] == {"type": "string", "maxLength": RAW_LENGTH_CAP}
    description = parameter["description"]
    assert '"f"' in description and '"o"' in description and '"v"' in description
    assert '{"any": [' in description
    assert "x-filter-schemas" in description


def test_the_filter_grammar_says_what_every_operator_and_kind_takes(
    tmp_path: Path,
):
    """A field's kind and operators (``x-filter-schemas``) do not say what goes in
    ``v``: how many values ``between`` takes, how a date or a boolean is written,
    how big a tree may grow. Without that sentence an API customer learned the
    grammar from 422s (api-consumer review, 2026-11-12). Each claim is pinned to
    the parser's own table, so the description cannot drift from what it refuses."""
    from cara.filtering import (
        LAST_DAYS_MAX,
        MAX_CONDITIONS,
        MAX_GROUP_CHILDREN,
        MAX_ROOT_NODES,
        OPERATOR_ARITY,
        RAW_LENGTH_CAP,
        VALUES_HARD_CAP,
    )
    from cara.openapi import request_query_parameters

    requests = _write_source(
        tmp_path,
        "requests",
        '''
class IndexRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {"filters": "nullable|string|filter_tree:things.index"}
''',
    )
    schema = FormRequestSchemaExtractor(requests).extract()["IndexRequest"]
    (parameter,) = request_query_parameters(schema)
    description = parameter["description"]

    by_arity: dict[tuple[int, int | None], set[str]] = {}
    for operator, arity in OPERATOR_ARITY.items():
        by_arity.setdefault(arity, set()).add(operator)
    assert by_arity.pop((1, None)) == {"in", "not_in"}
    assert by_arity.pop((2, 2)) == {"between"}
    assert by_arity.pop((0, 0)) == {"empty", "not_empty"}
    assert set(by_arity) == {(1, 1)}
    assert "`in` / `not_in` take one or more values" in description
    assert "`between` takes two" in description
    assert "`empty` / `not_empty` take none" in description
    assert "every other operator takes one" in description

    assert '"true" or "false"' in description
    assert "YYYY-MM-DD" in description
    assert f"from 1 to {LAST_DAYS_MAX}" in description
    assert "`prefix`" in description
    for ceiling in (
        f"{MAX_ROOT_NODES} top-level nodes",
        f"{MAX_GROUP_CHILDREN} conditions in a group",
        f"{MAX_CONDITIONS} conditions in all",
        f"{VALUES_HARD_CAP} values in a condition",
        f"{RAW_LENGTH_CAP} characters",
    ):
        assert ceiling in description, ceiling


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
