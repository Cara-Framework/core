"""The contract says what the code can send: ``data: null``, and either value of
a local assigned twice."""

from __future__ import annotations

from pathlib import Path

from cara.openapi import ResourceSchemaExtractor, null_data_actions


def _controllers(tmp_path: Path, source: str) -> Path:
    directory = tmp_path / "controllers"
    directory.mkdir()
    (directory / "ThingController.py").write_text(source, encoding="utf-8")
    return directory


def test_an_action_handing_the_envelope_none_answers_null(tmp_path):
    directory = _controllers(
        tmp_path,
        """
class ThingController:
    async def show(self, response):
        return await self._answer(response)

    async def _answer(self, response):
        return response.envelope(None)

    async def index(self, response):
        return response.envelope({"a": 1})
""",
    )
    assert null_data_actions(directory) == {"ThingController@show"}


def _status_schema(tmp_path: Path, body: str) -> dict:
    directory = tmp_path / "resources"
    directory.mkdir()
    (directory / "ThingResource.py").write_text(
        f"""
class ThingResource(BaseResource):
    def to_dict(self):
{body}
        return {{"status": status}}
""",
        encoding="utf-8",
    )
    schemas = ResourceSchemaExtractor(directory, {}).extract()
    return schemas["ThingResource"]["properties"]["status"]


def test_a_local_also_assigned_from_a_row_documents_no_values(tmp_path):
    schema = _status_schema(
        tmp_path,
        "        status = self.resource.status\n"
        '        if status == "pending":\n'
        '            status = "expired"',
    )
    assert "enum" not in schema


def test_a_local_assigned_two_literals_documents_both(tmp_path):
    schema = _status_schema(
        tmp_path,
        '        status = "open"\n'
        "        if self.resource.done:\n"
        '            status = "closed"',
    )
    assert schema["enum"] == ["closed", "open"]
