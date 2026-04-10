## Add test coverage for five untested validation rules

`EmailRule`, `URLRule`, `BooleanRule`, `AlphanumRule`, and `UuidRule` all have zero test coverage. Key details:
- `BooleanRule.validate` accepts `"yes"` and `"no"` as valid.
- `BooleanRule`, `AlphanumRule`, `UuidRule` override `message()` directly (not `default_message()`). `EmailRule` and `URLRule` override `default_message()`.
- `AlphanumRule` coerces non-string values to `str` before matching.
- All `validate()` calls use signature `(field, value, params_dict)`.
- Per lessons: never use `assert x is True` — use `assert x` / `assert not x`.

### tests/validation/test_basic_rules.py

```python
from cara.validation.rules.EmailRule import EmailRule
from cara.validation.rules.URLRule import URLRule
from cara.validation.rules.BooleanRule import BooleanRule
from cara.validation.rules.AlphanumRule import AlphanumRule
from cara.validation.rules.UuidRule import UuidRule


# --- EmailRule ---

def test_email_rule_valid():
    rule = EmailRule()
    assert rule.validate("email", "user@example.com", {})
    assert rule.validate("email", "alice+tag@sub.domain.org", {})


def test_email_rule_invalid():
    rule = EmailRule()
    assert not rule.validate("email", "not-an-email", {})
    assert not rule.validate("email", "@example.com", {})
    assert not rule.validate("email", "user@", {})
    assert not rule.validate("email", "", {})
    assert not rule.validate("email", None, {})
    assert not rule.validate("email", 123, {})


def test_email_rule_default_message():
    rule = EmailRule()
    msg = rule.default_message("email", {})
    assert "email" in msg.lower()


# --- URLRule ---

def test_url_rule_valid():
    rule = URLRule()
    assert rule.validate("website", "https://example.com", {})
    assert rule.validate("website", "http://example.com/path?q=1", {})
    assert rule.validate("website", "http://localhost:8080", {})


def test_url_rule_invalid():
    rule = URLRule()
    assert not rule.validate("website", "not-a-url", {})
    assert not rule.validate("website", "ftp://example.com", {})
    assert not rule.validate("website", "", {})
    assert not rule.validate("website", None, {})


def test_url_rule_default_message():
    rule = URLRule()
    msg = rule.default_message("website", {})
    assert "website" in msg.lower()


# --- BooleanRule ---

def test_boolean_rule_valid():
    rule = BooleanRule()
    for val in [True, False, 1, 0, "1", "0", "true", "false", "yes", "no"]:
        assert rule.validate("active", val, {}), f"Expected {val!r} to be valid"


def test_boolean_rule_invalid():
    rule = BooleanRule()
    assert not rule.validate("active", "maybe", {})
    assert not rule.validate("active", 2, {})
    assert not rule.validate("active", None, {})
    assert not rule.validate("active", [], {})


def test_boolean_rule_message():
    rule = BooleanRule()
    msg = rule.message("active", {})
    assert "active" in msg


# --- AlphanumRule ---

def test_alphanum_rule_valid():
    rule = AlphanumRule()
    assert rule.validate("code", "abc123", {})
    assert rule.validate("code", "HELLO", {})
    assert rule.validate("code", "42", {})


def test_alphanum_rule_coerces_int():
    rule = AlphanumRule()
    assert rule.validate("code", 123, {})


def test_alphanum_rule_invalid():
    rule = AlphanumRule()
    assert not rule.validate("code", "hello world", {})
    assert not rule.validate("code", "hello-world", {})
    assert not rule.validate("code", "", {})
    assert not rule.validate("code", None, {})


def test_alphanum_rule_message():
    rule = AlphanumRule()
    msg = rule.message("code", {})
    assert "code" in msg


# --- UuidRule ---

def test_uuid_rule_valid_with_hyphens():
    rule = UuidRule()
    assert rule.validate("id", "550e8400-e29b-41d4-a716-446655440000", {})


def test_uuid_rule_valid_without_hyphens():
    rule = UuidRule()
    assert rule.validate("id", "550e8400e29b41d4a716446655440000", {})


def test_uuid_rule_valid_uppercase():
    rule = UuidRule()
    assert rule.validate("id", "550E8400-E29B-41D4-A716-446655440000", {})


def test_uuid_rule_invalid():
    rule = UuidRule()
    assert not rule.validate("id", "not-a-uuid", {})
    assert not rule.validate("id", "550e8400-e29b-41d4-a716", {})
    assert not rule.validate("id", "", {})
    assert not rule.validate("id", None, {})


def test_uuid_rule_message():
    rule = UuidRule()
    msg = rule.message("id", {})
    assert "id" in msg
```
