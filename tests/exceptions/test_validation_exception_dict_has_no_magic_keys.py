"""A dict of validation errors is read as ``{field: [messages]}``, whatever its keys.

``ValidationException`` special-cased a dict carrying the key
``route_parameter_validation_failed`` (an old ``RouteParameterValidator``
shape that nothing produces any more): it then read only ``first_error`` and
``errors`` from it, so an errors dict holding that key answered a 422 with an
empty error map and the generic "Validation failed". The branch is gone; no
key of the dict is special.
"""

from __future__ import annotations

from cara.exceptions import ValidationException


def test_a_plain_errors_dict_is_the_error_map():
    exc = ValidationException({"sku": ["The sku is taken."], "qty": ["Too many."]})

    assert exc.errors == {"sku": ["The sku is taken."], "qty": ["Too many."]}
    assert str(exc) == "The sku is taken."
    assert exc.to_dict()["meta"]["failed_fields"] == ["sku", "qty"]


def test_the_old_route_parameter_marker_is_just_another_field():
    errors = {
        "route_parameter_validation_failed": ["The id must be an integer."],
        "id": ["The id must be an integer."],
    }

    exc = ValidationException(errors)

    assert exc.errors == errors
    assert str(exc) == "The id must be an integer."
    assert exc.get_error_count() == 2


def test_first_error_and_errors_keys_are_not_read_as_an_envelope():
    errors = {
        "route_parameter_validation_failed": ["marker"],
        "first_error": ["should not become the message"],
        "errors": ["is a field here"],
    }

    exc = ValidationException(errors)

    assert exc.errors == errors
    assert str(exc) == "marker"
