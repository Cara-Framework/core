"""Resource introspection — pin the field set, and admit what cannot be seen.

The value of a generated spec is not that it is complete; it is that it never
lies. Two failure modes are worse than a missing key: publishing a CLOSED
schema for a payload that is only partly visible, and dropping keys that ARE
visible because the serializer happened to return a variable. Both were live
bugs in the hand-rolled generators this module replaced, and both are pinned
here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi import ResourceSchemaExtractor


def _resources(tmp_path: Path, **modules: str) -> Path:
    directory = tmp_path / "resources"
    directory.mkdir()
    for name, source in modules.items():
        (directory / f"{name}.py").write_text(source, encoding="utf-8")
    return directory


class TestFieldExtraction:
    def test_reads_the_returned_dict_literal_with_types(self, tmp_path: Path):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                RowResource="""
class RowResource:
    def to_array(self, request=None):
        return {
            "id": self.opt_int(self.resource.id),
            "name": self.opt_str(self.resource.name),
            "score": self.opt_float(self.resource.score),
            "created_at": self.opt_datetime(self.resource.created_at),
            "raw": self.resource.whatever,
        }
""",
            )
        ).extract()

        row = schemas["RowResource"]
        assert row["properties"]["id"] == {"type": "integer", "nullable": True}
        assert row["properties"]["name"] == {"type": "string", "nullable": True}
        assert row["properties"]["created_at"] == {
            "type": "string",
            "format": "date-time",
            "nullable": True,
        }
        # An expression the extractor cannot read is permissive, NOT invented.
        assert row["properties"]["raw"] == {}
        assert "additionalProperties" not in row

    def test_captures_the_literal_that_built_a_returned_variable(self, tmp_path: Path):
        """A resource that returns a VARIABLE still publishes its full shape.

        Reading only ``return {...}`` and the later ``payload["k"] = ...``
        assignments would publish a schema with exactly one key while claiming
        to be closed — a confident wrong answer about a 3-key payload.
        """
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                DetailResource="""
class DetailResource:
    def to_array(self, request=None):
        payload = {
            "id": self.opt_int(self.resource.id),
            "title": self.opt_str(self.resource.title),
        }
        payload["extra"] = self.opt_bool(self.resource.extra)
        return payload
""",
            )
        ).extract()

        detail = schemas["DetailResource"]
        assert list(detail["properties"]) == ["id", "title", "extra"]
        assert "additionalProperties" not in detail

    def test_resolves_a_key_through_an_earlier_typed_local(self, tmp_path: Path):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                PrecomputedResource="""
class PrecomputedResource:
    def to_array(self, request=None):
        amount = self.opt_float(self.resource.amount)
        return {"amount": amount}
""",
            )
        ).extract()

        assert schemas["PrecomputedResource"]["properties"]["amount"] == {
            "type": "number",
            "nullable": True,
        }

    def test_nested_resource_references_become_refs(self, tmp_path: Path):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                ParentResource="""
class ParentResource:
    def to_array(self, request=None):
        return {
            "child": ChildResource(self.resource.child).to_array(),
            "children": ChildResource.collection(self.resource.children),
        }
""",
            )
        ).extract()

        properties = schemas["ParentResource"]["properties"]
        assert properties["child"] == {"$ref": "#/components/schemas/ChildResource"}
        assert properties["children"] == {
            "type": "array",
            "items": {"$ref": "#/components/schemas/ChildResource"},
        }


class TestHonestOpenness:
    @pytest.mark.parametrize(
        "body",
        [
            # Payload continues a model's own serialization.
            'data = self.resource.serialize()\n        data["x"] = 1\n        return data',
            # Payload continues the PARENT serializer: every inherited key is
            # emitted too, so the subclass schema cannot be closed.
            'data = super().to_array(request)\n        data["x"] = 1\n        return data',
            # A key computed at run time: keys this cannot name will appear.
            'data = {"a": 1}\n        data[key] = 2\n        return data',
            # Merged from a value the extractor cannot read.
            'data = {"a": 1}\n        data.update(self._bundle())\n        return data',
        ],
    )
    def test_a_payload_it_cannot_fully_see_is_left_open(self, tmp_path: Path, body: str):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                PartialResource=f"""
class PartialResource:
    def to_array(self, request=None):
        {body}
""",
            )
        ).extract()

        partial = schemas["PartialResource"]
        assert partial["additionalProperties"] is True
        assert partial["x-fields-partial"] is True

    def test_a_class_without_a_serializer_emits_nothing(self, tmp_path: Path):
        schemas = ResourceSchemaExtractor(
            _resources(tmp_path, PlainThing="class PlainThing:\n    pass\n")
        ).extract()

        assert schemas == {}


class TestApplicationCoercionHelpers:
    def test_an_unregistered_house_helper_degrades_instead_of_guessing(
        self, tmp_path: Path
    ):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                HouseResource="""
class HouseResource:
    def to_array(self, request=None):
        return {"blob": self.opt_dict(self.resource.blob)}
""",
            )
        ).extract()

        # ``opt_dict`` is not a framework helper. Guessing a type from a name
        # the framework does not own is exactly how a spec starts lying.
        assert schemas["HouseResource"]["properties"]["blob"] == {}

    def test_a_registered_house_helper_types_its_keys(self, tmp_path: Path):
        schemas = ResourceSchemaExtractor(
            _resources(
                tmp_path,
                HouseResource="""
class HouseResource:
    def to_array(self, request=None):
        return {"blob": self.opt_dict(self.resource.blob)}
""",
            ),
            {"opt_dict": {"type": "object", "nullable": True}},
        ).extract()

        assert schemas["HouseResource"]["properties"]["blob"] == {
            "type": "object",
            "nullable": True,
        }
