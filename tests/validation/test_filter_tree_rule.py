"""``filter_tree:<schema>`` rule — the request-boundary door.

The rule validates through the SAME parser the controller re-parses
with, so acceptance here is acceptance everywhere. A failing payload
surfaces the parser's path-precise message, not a generic shrug.
"""

from __future__ import annotations

import json

from cara.filtering import TreeField, TreeSchema, register_tree_schema
from cara.validation import Validation

_RULES = {"filters": "nullable|string|filter_tree:tests.rule"}


def _register() -> None:
    register_tree_schema(
        TreeSchema(
            "tests.rule",
            (
                TreeField(
                    "status",
                    "select",
                    column="status",
                    options=(("active", "Live"), ("draft", "Draft")),
                ),
            ),
        )
    )


def test_valid_and_absent_payloads_pass():
    _register()
    payload = json.dumps([{"f": "status", "o": "in", "v": ["active"]}])
    assert Validation.make({"filters": payload}, _RULES).passes() is True
    assert Validation.make({}, _RULES).passes() is True


def test_invalid_payload_fails_with_the_parser_message():
    _register()
    payload = json.dumps([{"f": "bogus", "o": "in", "v": ["x"]}])
    validator = Validation.make({"filters": payload}, _RULES)
    assert validator.passes() is False
    messages = json.dumps(validator.errors().all())
    assert "unknown field" in messages


def test_unregistered_schema_fails_closed():
    validator = Validation.make(
        {"filters": "[]"},
        {"filters": "nullable|string|filter_tree:tests.nowhere"},
    )
    assert validator.passes() is False
