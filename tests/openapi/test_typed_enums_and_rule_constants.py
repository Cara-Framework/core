"""Request rules publish typed enums, dated formats and f-string constants.

The live defects:

* ``integer|in:1,7`` published ``enum: ["1", "7"]`` — strings no valid payload
  matches (and a generated client that sends strings); ``numeric|in:`` and
  ``boolean|in:`` were strings too.
* ``date_format:%Y-%m-%d`` (the rule's registered name) was not recognised:
  no ``type: string``, no ``format: date``.
* A rule CONSTANT built as an f-string (``LANE_RULE = f"in:{','.join(KEYS)}"``)
  resolved to nothing, so the published rule stayed partial.
* An unimportable module of request constants was skipped by a silent
  ``except``; it now leaves a warning.
"""

from __future__ import annotations

import ast
import importlib
import logging
from pathlib import Path

from cara.openapi import FormRequestSchemaExtractor

constants = importlib.import_module("cara.openapi._RequestConstants")


def _properties(tmp_path: Path, source: str) -> dict:
    root = tmp_path / "requests"
    root.mkdir()
    (root / "Contracts.py").write_text(source, encoding="utf-8")
    return FormRequestSchemaExtractor(root).extract()["FilterRequest"]["properties"]


def test_in_values_are_published_in_the_fields_own_type(tmp_path: Path):
    properties = _properties(
        tmp_path,
        '''
class FilterRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {
            "days": "nullable|integer|in:1,7,30",
            "ratio": "nullable|numeric|in:0.5,1",
            "active": "nullable|boolean|in:true,false",
            "status": "nullable|string|in:open,closed",
        }
''',
    )

    assert properties["days"]["enum"] == [1, 7, 30]
    assert properties["ratio"]["enum"] == [0.5, 1]
    assert properties["active"]["enum"] == [True, False]
    assert properties["status"]["enum"] == ["open", "closed"]


def test_date_format_is_recognised_as_a_date_string(tmp_path: Path):
    properties = _properties(
        tmp_path,
        '''
class FilterRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {
            "since": "nullable|date_format:%Y-%m-%d",
            "stamp": "nullable|date_format:%Y-%m-%dT%H:%M",
        }
''',
    )

    assert properties["since"]["type"] == "string"
    assert properties["since"]["format"] == "date"
    assert properties["since"]["x-cara-date-format"] == "%Y-%m-%d"
    # Another format is a string, but not a ``date``.
    assert properties["stamp"]["type"] == "string"
    assert "format" not in properties["stamp"]


def test_an_fstring_rule_constant_is_resolved(tmp_path: Path):
    properties = _properties(
        tmp_path,
        '''
LANE_KEYS = ("north", "south")
LANE_RULE = f"nullable|string|in:{','.join(LANE_KEYS)}"


class FilterRequest(FormRequest):
    """GET /things query params."""

    def rules(self):
        return {"lane": LANE_RULE}
''',
    )

    assert properties["lane"]["enum"] == ["north", "south"]


def test_an_fstring_with_an_unresolvable_placeholder_stays_unresolved():
    values = constants._module_values(
        ast.parse("RULE = f'in:{runtime_lanes()}'\nOK = f'max:{5:d}'")
    )

    assert "RULE" not in values
    assert values["OK"] == "max:5"


def test_an_unimportable_constants_module_is_logged_not_swallowed(tmp_path: Path, caplog):
    tree = ast.parse("from nowhere_cara_test.lanes import LANE_KEYS\n")
    path = tmp_path / "requests" / "Contracts.py"

    with caplog.at_level(logging.WARNING, logger="cara.openapi"):
        found = constants._external_values(tree, path, tmp_path / "requests", frozenset())

    assert found == {}
    assert any(
        "nowhere_cara_test.lanes" in record.getMessage()
        and record.levelno == logging.WARNING
        for record in caplog.records
    )
