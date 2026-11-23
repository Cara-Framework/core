"""``RouteParameterValidator`` coerces and nothing else.

``validate_parameters`` survived the removal of the rule registry it read:
no caller anywhere, and it called ``get_all_rules_for_parameter``, which no
longer exists, so any call raised ``AttributeError``.
"""

from __future__ import annotations

from cara.routing.RouteParameterValidator import RouteParameterValidator


def test_the_dead_validation_entry_point_is_gone():
    assert not hasattr(RouteParameterValidator, "validate_parameters")
    assert not hasattr(RouteParameterValidator, "_extract_validation_errors")


def test_coercion_still_works(monkeypatch):
    monkeypatch.setattr(RouteParameterValidator, "_compile_rules", {"id": "int"})

    assert RouteParameterValidator.convert_parameter_value("id", "42") == 42
    assert RouteParameterValidator.convert_parameter_value("id", "x") == "x"
    assert RouteParameterValidator.convert_parameter_value("other", "7") == "7"
