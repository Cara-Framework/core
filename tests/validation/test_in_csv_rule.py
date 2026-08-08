"""InCsvRule — the multi-value (`IN (...)`) shape of an index filter param.

A CSV value passes only when EVERY token is in the allowlist; a single
value is the one-token case of the same shape, so a parameter can graduate
from ``in:`` to ``in_csv:`` without breaking existing callers.
"""

from cara.validation.rules import InCsvRule
from cara.validation.Validation import Validation


def test_in_csv_rule_accepts_single_and_multi_tokens():
    rule = InCsvRule()
    params = {"in_csv": "draft,active,ended"}
    assert rule.validate("status", "draft", params) is True
    assert rule.validate("status", "draft,active", params) is True
    assert rule.validate("status", "ended,draft,active", params) is True


def test_in_csv_rule_rejects_unknown_empty_and_nonscalar():
    rule = InCsvRule()
    params = {"in_csv": "draft,active"}
    assert rule.validate("status", "draft,bogus", params) is False
    # An empty token (``a,,b`` or a bare ``,``) is a malformed filter, not
    # an empty one — absence is spelled by omitting the parameter.
    assert rule.validate("status", "", params) is False
    assert rule.validate("status", "draft,,active", params) is False
    assert rule.validate("status", ",", params) is False
    assert rule.validate("status", None, params) is False
    # The InRule scalar guard carries over verbatim.
    assert rule.validate("status", ["draft"], params) is False
    assert rule.validate("status", {"draft": 1}, params) is False
    assert rule.validate("status", b"draft", params) is False


def test_in_csv_rule_resolves_from_rule_strings():
    rules = {"status": "nullable|string|in_csv:draft,active"}
    assert Validation.make({"status": "draft,active"}, rules).passes() is True
    assert Validation.make({"status": "draft,bogus"}, rules).passes() is False
    assert Validation.make({}, rules).passes() is True


def test_in_csv_rule_default_message_names_the_allowlist():
    rule = InCsvRule()
    message = rule.default_message("status", {"in_csv": "draft,active"})
    assert "status" in message
    assert "draft,active" in message
