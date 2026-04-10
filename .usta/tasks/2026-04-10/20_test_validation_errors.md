## Add test coverage for ValidationErrors

`cara/validation/ValidationErrors.py` has zero test coverage. Add a focused test file covering the core API.

### tests/validation/test_validation_errors.py

```python
import json
from cara.validation.ValidationErrors import ValidationErrors


def _sample_errors():
    return ValidationErrors({
        "email": ["The email field is required.", "The email must be valid."],
        "name": ["The name field is required."],
    })


def test_first_with_field():
    errors = _sample_errors()
    assert errors.first("email") == "The email field is required."
    assert errors.first("name") == "The name field is required."
    assert errors.first("missing") == ""


def test_first_without_field():
    errors = _sample_errors()
    assert errors.first() == "The email field is required."


def test_first_error_alias():
    errors = _sample_errors()
    assert errors.first_error() == errors.first()


def test_all_returns_copy():
    errors = _sample_errors()
    result = errors.all()
    assert result == errors._errors
    assert result is not errors._errors


def test_has():
    errors = _sample_errors()
    assert errors.has("email") is True
    assert errors.has("missing") is False


def test_get():
    errors = _sample_errors()
    assert errors.get("email") == ["The email field is required.", "The email must be valid."]
    assert errors.get("missing") == []


def test_count():
    errors = _sample_errors()
    assert errors.count("email") == 2
    assert errors.count("name") == 1
    assert errors.count("missing") == 0
    assert errors.count() == 3


def test_messages():
    errors = _sample_errors()
    msgs = errors.messages()
    assert len(msgs) == 3
    assert "The name field is required." in msgs


def test_keys():
    errors = _sample_errors()
    assert set(errors.keys()) == {"email", "name"}


def test_empty_and_any():
    errors = _sample_errors()
    assert errors.empty() is False
    assert errors.any() is True

    empty = ValidationErrors({})
    assert empty.empty() is True
    assert empty.any() is False


def test_only():
    errors = _sample_errors()
    result = errors.only("email")
    assert "email" in result
    assert "name" not in result


def test_except():
    errors = _sample_errors()
    result = errors.except_("email")
    assert "email" not in result
    assert "name" in result


def test_to_json():
    errors = _sample_errors()
    parsed = json.loads(errors.to_json())
    assert parsed["email"] == ["The email field is required.", "The email must be valid."]


def test_dunder_methods():
    errors = _sample_errors()
    assert bool(errors) is True
    assert len(errors) == 3
    assert "email" in errors
    assert "missing" not in errors
    assert set(errors) == {"email", "name"}

    empty = ValidationErrors({})
    assert bool(empty) is False
    assert len(empty) == 0
    assert str(empty) == "No validation errors"


def test_str_representation():
    errors = ValidationErrors({"age": ["Too young."]})
    assert str(errors) == "age: Too young."
```
