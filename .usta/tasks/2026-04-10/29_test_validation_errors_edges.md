## Add edge-case tests for ValidationErrors

The existing `tests/validation/test_validation_errors.py` covers the happy path. Add edge cases: multiple fields in `only()`/`except_()`, `errors()` alias, `to_dict()` alias, `repr()`, and multi-line `__str__`.

### tests/validation/test_validation_errors.py

Append these new test functions at the end of the file (do NOT remove or modify any existing tests):

```python
def test_errors_alias():
    errors = _sample_errors()
    assert errors.errors() == errors.all()


def test_to_dict_alias():
    errors = _sample_errors()
    assert errors.to_dict() == errors.all()


def test_repr():
    errors = ValidationErrors({"age": ["Too young."]})
    r = repr(errors)
    assert "ValidationErrors" in r
    assert "age" in r


def test_only_multiple_fields():
    errors = _sample_errors()
    result = errors.only("email", "name")
    assert "email" in result
    assert "name" in result


def test_only_with_missing_field():
    errors = _sample_errors()
    result = errors.only("email", "nonexistent")
    assert "email" in result
    assert "nonexistent" not in result


def test_except_multiple_fields():
    errors = _sample_errors()
    result = errors.except_("email", "name")
    assert len(result) == 0


def test_str_multiline():
    errors = _sample_errors()
    s = str(errors)
    lines = s.strip().split("\n")
    assert len(lines) == 3


def test_first_on_empty():
    errors = ValidationErrors({})
    assert errors.first() == ""
    assert errors.first("any") == ""
```
