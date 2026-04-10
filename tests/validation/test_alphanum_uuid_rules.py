from cara.validation.rules.AlphanumRule import AlphanumRule
from cara.validation.rules.UuidRule import UuidRule


def test_alphanum_rule_valid():
    rule = AlphanumRule()
    assert rule.validate("code", "abc123", {}) is True
    assert rule.validate("code", "HELLO", {}) is True
    assert rule.validate("code", "42", {}) is True


def test_alphanum_rule_invalid():
    rule = AlphanumRule()
    assert rule.validate("code", "hello world", {}) is False
    assert rule.validate("code", "hello-world", {}) is False
    assert rule.validate("code", "hello_world", {}) is False
    assert rule.validate("code", "", {}) is False
    assert rule.validate("code", None, {}) is False


def test_alphanum_rule_coerces_non_string():
    rule = AlphanumRule()
    assert rule.validate("code", 123, {}) is True


def test_alphanum_rule_message():
    rule = AlphanumRule()
    msg = rule.message("code", {})
    assert "code" in msg


def test_uuid_rule_valid_with_hyphens():
    rule = UuidRule()
    assert rule.validate("id", "550e8400-e29b-41d4-a716-446655440000", {}) is True


def test_uuid_rule_valid_without_hyphens():
    rule = UuidRule()
    assert rule.validate("id", "550e8400e29b41d4a716446655440000", {}) is True


def test_uuid_rule_valid_uppercase():
    rule = UuidRule()
    assert rule.validate("id", "550E8400-E29B-41D4-A716-446655440000", {}) is True


def test_uuid_rule_invalid():
    rule = UuidRule()
    assert rule.validate("id", "not-a-uuid", {}) is False
    assert rule.validate("id", "550e8400-e29b-41d4-a716", {}) is False
    assert rule.validate("id", "", {}) is False
    assert rule.validate("id", None, {}) is False


def test_uuid_rule_message():
    rule = UuidRule()
    msg = rule.message("id", {})
    assert "id" in msg
