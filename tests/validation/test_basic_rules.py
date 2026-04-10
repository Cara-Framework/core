from cara.validation.rules.EmailRule import EmailRule
from cara.validation.rules.URLRule import URLRule
from cara.validation.rules.BooleanRule import BooleanRule


def test_email_rule_valid():
    rule = EmailRule()
    assert rule.validate("email", "user@example.com", {}) is True
    assert rule.validate("email", "alice+tag@sub.domain.org", {}) is True


def test_email_rule_invalid():
    rule = EmailRule()
    assert rule.validate("email", "not-an-email", {}) is False
    assert rule.validate("email", "@example.com", {}) is False
    assert rule.validate("email", "user@", {}) is False
    assert rule.validate("email", "", {}) is False
    assert rule.validate("email", None, {}) is False


def test_email_rule_default_message():
    rule = EmailRule()
    msg = rule.default_message("email", {})
    assert "email" in msg.lower()


def test_url_rule_valid():
    rule = URLRule()
    assert rule.validate("website", "https://example.com", {}) is True
    assert rule.validate("website", "http://example.com/path?q=1", {}) is True
    assert rule.validate("website", "http://localhost:8080", {}) is True


def test_url_rule_invalid():
    rule = URLRule()
    assert rule.validate("website", "not-a-url", {}) is False
    assert rule.validate("website", "ftp://example.com", {}) is False
    assert rule.validate("website", "", {}) is False
    assert rule.validate("website", None, {}) is False


def test_url_rule_default_message():
    rule = URLRule()
    msg = rule.default_message("website", {})
    assert "website" in msg.lower()


def test_boolean_rule_valid():
    rule = BooleanRule()
    for val in [True, False, 1, 0, "1", "0", "true", "false", "yes", "no"]:
        assert rule.validate("active", val, {}) is True, f"Expected {val!r} to be valid"


def test_boolean_rule_invalid():
    rule = BooleanRule()
    assert rule.validate("active", "maybe", {}) is False
    assert rule.validate("active", 2, {}) is False
    assert rule.validate("active", None, {}) is False
    assert rule.validate("active", [], {}) is False


def test_boolean_rule_message():
    rule = BooleanRule()
    msg = rule.message("active", {})
    assert "active" in msg
