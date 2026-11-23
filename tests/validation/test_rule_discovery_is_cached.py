"""The built-in rule classes are scanned from disk once per process.

``Validation.__init__`` used to list ``cara/validation/rules/`` and sweep every
rule module with ``inspect.getmembers`` on EVERY instantiation — 320 µs before
a rule ran, paid by each ``Validation.make`` (a FormRequest, ``request.validate``,
every ``validated_query_int`` read). These tests pin the scan count, and that
caching it changed nothing a caller can see.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

from cara.validation import Validation
from cara.validation.contracts import Rule

# The MODULE, not the class: the package barrel re-exports ``Validation`` under
# the submodule's own name, so ``import cara.validation.Validation as m`` binds
# the class.
validation_module = importlib.import_module("cara.validation.Validation")


@pytest.fixture
def cold_rule_cache(monkeypatch):
    """Start from an unscanned registry, and restore whatever was there."""
    monkeypatch.setattr(Validation, "_builtin_rules", None)


def test_rule_package_is_listed_once_for_many_validations(cold_rule_cache, monkeypatch):
    listings: list[str] = []
    real_listdir = validation_module.os.listdir

    def counting_listdir(path):
        listings.append(str(path))
        return real_listdir(path)

    monkeypatch.setattr(validation_module.os, "listdir", counting_listdir)

    for _ in range(25):
        validator = Validation.make({"age": 30}, {"age": "required|integer|min:18"})
        assert validator.passes()

    rule_dir_listings = [path for path in listings if path.endswith("/rules")]
    assert len(rule_dir_listings) == 1


def test_cached_registry_holds_the_same_rules_a_fresh_scan_finds(cold_rule_cache):
    scanned = Validation()._scan_rule_package()
    served = Validation()._discover_rules()

    assert served == scanned
    assert {"required", "integer", "alpha_num", "required_if", "distinct"} <= set(served)


def test_each_validator_owns_its_registry(cold_rule_cache):
    first = Validation()
    first._Validation__rule_classes.pop("required")

    second = Validation.make({"name": ""}, {"name": "required"})

    assert second.fails()
    assert "required" in Validation._builtin_rules


def test_extend_after_the_scan_reaches_new_validators(cold_rule_cache, monkeypatch):
    class ShoutingRule(Rule):
        def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
            return isinstance(value, str) and value.isupper()

        def message(self, field: str, params: dict[str, Any]) -> str:
            return f"{field} must shout."

    assert Validation.make({"name": "x"}, {"name": "string"}).passes()  # warm the cache
    monkeypatch.setattr(Validation, "_custom_rules", dict(Validation._custom_rules))
    Validation.extend("shouting", ShoutingRule)

    assert Validation.make({"name": "LOUD"}, {"name": "shouting"}).passes()
    quiet = Validation.make({"name": "quiet"}, {"name": "shouting"})
    assert quiet.errors().all() == {"name": ["name must shout."]}
    # The custom rule lives in ``_custom_rules``, never in the built-in scan.
    assert "shouting" not in Validation._builtin_rules
