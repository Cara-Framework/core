"""What the generator reads off ``.with_status``, parent payloads and row lists.

Each of these wrote a confident wrong answer into the published contract: a
door documented 200 that answers 201, a detail resource documented without its
parent's keys, and a list documented as one object.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi import (
    ControllerActionMapper,
    ControllerContractExtractor,
    ResourceSchemaExtractor,
)
from cara.openapi.Inference import infer_value_schema


def _tree(root: Path, folder: str, **modules: str) -> Path:
    directory = root / folder
    directory.mkdir()
    for name, source in modules.items():
        (directory / f"{name}.py").write_text(source, encoding="utf-8")
    return directory


def _statuses(tmp_path: Path, source: str) -> set[int]:
    contracts = ControllerContractExtractor(
        _tree(tmp_path, "controllers", ThingController=source)
    ).extract()
    return {variant.status for variant in contracts["ThingController@store"].responses}


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("return R(row).with_status(201).to_response(response)", {201}),
        (
            "return R(row).with_status(202 if queued else 200).to_response(response)",
            {200, 202},
        ),
        ("return R(row).with_meta(m).with_status(202).to_response(response)", {202}),
        ("return R(row).with_status(HTTPStatus.CREATED).to_response(response)", {201}),
        ("return R(row).to_response(response)", {200}),
        (
            "code = 202 if queued else 201\n"
            "        return R(row).with_status(code).to_response(response)",
            {201, 202},
        ),
    ],
)
def test_a_resource_status_is_the_status_documented(tmp_path, body, expected):
    source = f"""
class ThingController:
    async def store(self, request, response):
        {body}
"""
    assert _statuses(tmp_path, source) == expected


def test_a_status_set_on_a_variable_keeps_the_plain_answer_beside_it(tmp_path):
    source = """
class ThingController:
    async def store(self, request, response):
        resource = R(row)
        if created:
            resource = resource.with_status(201)
        return resource.to_response(response)
"""
    assert _statuses(tmp_path, source) == {200, 201}


def test_a_status_passed_to_a_shared_helper_reaches_the_action(tmp_path):
    source = """
class ThingController:
    async def store(self, request, response):
        return await self._respond(response, row, status=201)

    async def _respond(self, response, row, *, status: int = 200):
        resource = R(row)
        if status != 200:
            resource = resource.with_status(status)
        return resource.to_response(response)
"""
    assert _statuses(tmp_path, source) == {200, 201}


def _schemas(tmp_path: Path, source: str) -> dict:
    return ResourceSchemaExtractor(_tree(tmp_path, "resources", All=source)).extract()


_PARENT = """
class ParentResource:
    def to_dict(self):
        return {"id": self.opt_str(self.resource.id), "status": self.resource.status}
"""


@pytest.mark.parametrize(
    "child",
    [
        """
class ChildResource(ParentResource):
    def to_dict(self):
        return {**super().to_dict(), "items": []}
""",
        """
class ChildResource(ParentResource):
    def to_dict(self):
        payload = super().to_dict()
        payload["items"] = []
        return payload
""",
        """
class ChildResource:
    def to_dict(self):
        payload = ParentResource(self.resource).to_dict()
        payload["items"] = []
        return payload
""",
    ],
)
def test_a_payload_that_continues_a_resource_documents_its_keys(tmp_path, child):
    schema = _schemas(tmp_path, _PARENT + child)["ChildResource"]

    assert set(schema["properties"]) == {"id", "status", "items"}
    assert set(schema["required"]) == {"id", "status", "items"}
    assert "additionalProperties" not in schema


def test_a_chain_of_parents_folds_in_any_declaration_order(tmp_path):
    grandchild = """
class GrandchildResource(ChildResource):
    def to_dict(self):
        payload = super().to_dict()
        payload["deep"] = 1
        return payload

class ChildResource(ParentResource):
    def to_dict(self):
        payload = super().to_dict()
        payload["items"] = []
        return payload
"""
    schema = _schemas(tmp_path, grandchild + _PARENT)["GrandchildResource"]

    assert set(schema["properties"]) == {"id", "status", "items", "deep"}


def test_a_key_the_child_deletes_is_not_inherited(tmp_path):
    schema = _schemas(
        tmp_path,
        _PARENT
        + """
class ChildResource(ParentResource):
    def to_dict(self):
        payload = super().to_dict()
        del payload["status"]
        return payload
""",
    )["ChildResource"]

    assert set(schema["properties"]) == {"id"}
    assert schema["required"] == ["id"]


def test_a_key_the_child_sometimes_pops_is_optional(tmp_path):
    schema = _schemas(
        tmp_path,
        _PARENT
        + """
class ChildResource(ParentResource):
    def to_dict(self):
        payload = super().to_dict()
        if self.hidden:
            payload.pop("status", None)
        return payload
""",
    )["ChildResource"]

    assert set(schema["properties"]) == {"id", "status"}
    assert schema["required"] == ["id"]


def test_a_parent_it_cannot_read_leaves_the_child_open(tmp_path):
    schema = _schemas(
        tmp_path,
        """
class ChildResource(SomewhereElse):
    def to_dict(self):
        return {**super().to_dict(), "items": []}
""",
    )["ChildResource"]

    assert schema["additionalProperties"] is True
    assert schema["x-fields-partial"] is True


def test_an_open_parent_leaves_the_child_open(tmp_path):
    schema = _schemas(
        tmp_path,
        """
class ParentResource:
    def to_dict(self):
        return self.resource.serialize()

class ChildResource(ParentResource):
    def to_dict(self):
        payload = super().to_dict()
        payload["items"] = []
        return payload
""",
    )["ChildResource"]

    assert schema["additionalProperties"] is True


def test_a_helper_spread_documents_its_keys_as_optional(tmp_path):
    schema = _schemas(
        tmp_path,
        """
class RowResource:
    def to_dict(self):
        return {"id": 1, **self._extra(self.resource)}

    @staticmethod
    def _extra(row):
        if row is None:
            return {}
        return {"family": {"id": 1}}
""",
    )["RowResource"]

    assert set(schema["properties"]) == {"id", "family"}
    assert schema["required"] == ["id"]
    assert "additionalProperties" not in schema


def test_a_spread_it_cannot_read_leaves_the_schema_open(tmp_path):
    schema = _schemas(
        tmp_path,
        """
class RowResource:
    def to_dict(self):
        return {"id": 1, **build_elsewhere(self.resource)}
""",
    )["RowResource"]

    assert schema["additionalProperties"] is True


def test_a_fallback_object_does_not_close_the_real_values_keys():
    import ast

    node = ast.parse('row.get("write_state") or {"writable": True}', mode="eval").body

    schema = infer_value_schema(node)

    assert schema["additionalProperties"] is True
    assert set(schema["properties"]) == {"writable"}


def _mapping(tmp_path: Path, source: str) -> dict:
    return ControllerActionMapper(
        _tree(tmp_path, "controllers", ThingController=source), {"RowResource"}
    ).map()


@pytest.mark.parametrize(
    "body",
    [
        "return response.envelope([RowResource(r).to_dict() for r in rows])",
        "return response.envelope([RowResource(r) for r in rows], {'total': 1})",
        "out = [RowResource(r).to_dict() for r in rows]\n"
        "        return response.envelope(out)",
        "return response.paginated([RowResource(r).to_dict() for r in rows], limit=1)",
    ],
)
def test_rows_handed_to_the_response_are_a_list(tmp_path, body):
    source = f"""
class ThingController:
    async def index(self, request, response):
        {body}
"""
    assert _mapping(tmp_path, source)["ThingController@index"] == ("RowResource", True)


def test_rows_inside_the_data_do_not_describe_the_data(tmp_path):
    source = """
class ThingController:
    async def index(self, request, response):
        return response.envelope(
            {"sent": [RowResource(r).to_dict() for r in rows], "refused": 1}
        )
"""
    assert "ThingController@index" not in _mapping(tmp_path, source)


def test_a_resource_under_one_key_does_not_describe_the_data(tmp_path):
    """A hold answer (``{"operation": id, "approval": Resource(a).to_dict()}``)
    typed the whole data as that resource, so the door's real 200 body failed
    its own contract (bulk apply, gate 2026-11-14)."""
    source = """
class ThingController:
    async def apply(self, request, response):
        return response.envelope(
            {"operation": "OP1", "approval": RowResource(a).to_dict()}, status=202
        )
"""
    assert "ThingController@apply" not in _mapping(tmp_path, source)


def test_a_spread_resource_is_still_the_data(tmp_path):
    source = """
class ThingController:
    async def show(self, request, response):
        return response.envelope({**RowResource(a).to_dict(), "extra": 1})
"""
    assert _mapping(tmp_path, source)["ThingController@show"] == ("RowResource", False)
