## Add test coverage for MessageFormatter

`cara/validation/MessageFormatter.py` has zero test coverage. Add a focused test file. Note the correct signatures: `has_custom_message(params)` and `get_custom_message(params)` both take a single `params` dict. `has_custom_message` returns a truthy/falsy value (not necessarily literal `bool`), so use `assert` / `assert not` instead of `assert x is True`.

### tests/validation/test_message_formatter.py

```python
from cara.validation.MessageFormatter import MessageFormatter


def test_format_message_replaces_attribute():
    result = MessageFormatter.format_message(
        "The :attribute field is required.", "email", {}
    )
    assert result == "The Email field is required."


def test_format_message_replaces_field_placeholder():
    result = MessageFormatter.format_message(
        "Raw field is :field.", "user_name", {}
    )
    assert result == "Raw field is user_name."


def test_format_message_replaces_rule_specific_params():
    result = MessageFormatter.format_message(
        "The :attribute must be at least :min chars.",
        "name",
        {"min": "3"},
    )
    assert result == "The Name must be at least 3 chars."


def test_format_message_empty_returns_empty():
    result = MessageFormatter.format_message("", "email", {})
    assert result == ""


def test_format_attribute_name():
    assert MessageFormatter.format_attribute_name("first_name") == "First Name"
    assert MessageFormatter.format_attribute_name("email") == "Email"
    assert MessageFormatter.format_attribute_name("a_b_c") == "A B C"


def test_has_custom_message_present():
    params = {"_custom_message": "Please provide email."}
    assert MessageFormatter.has_custom_message(params)


def test_has_custom_message_missing():
    assert not MessageFormatter.has_custom_message({})
    assert not MessageFormatter.has_custom_message({"_custom_message": ""})


def test_get_custom_message():
    params = {"_custom_message": "We need your email."}
    assert MessageFormatter.get_custom_message(params) == "We need your email."


def test_get_custom_message_missing():
    assert MessageFormatter.get_custom_message({}) == ""
```
