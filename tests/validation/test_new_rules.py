
from cara.validation.rules import MinLengthRule, MaxLengthRule, PhoneRule, SlugRule


def test_min_length_rule():
    rule = MinLengthRule()

    # Valid cases
    assert rule.validate("name", "hello", {"min_length": "5"}) is True
    assert rule.validate("name", "hello world", {"min_length": "5"}) is True

    # Invalid cases
    assert rule.validate("name", "hi", {"min_length": "5"}) is False
    assert rule.validate("name", None, {"min_length": "5"}) is False
    assert rule.validate("name", 123, {"min_length": "2"}) is False

    # Test message
    message = rule.default_message("name", {"min_length": "5"})
    assert isinstance(message, str)
    assert "name" in message
    assert "5" in message


def test_max_length_rule():
    rule = MaxLengthRule()

    # Valid cases
    assert rule.validate("name", "hi", {"max_length": "5"}) is True
    assert rule.validate("name", "hello", {"max_length": "5"}) is True

    # Invalid cases
    assert rule.validate("name", "hello world", {"max_length": "5"}) is False
    assert rule.validate("name", None, {"max_length": "5"}) is False

    # Test message
    message = rule.default_message("name", {"max_length": "5"})
    assert isinstance(message, str)
    assert "name" in message
    assert "5" in message


def test_phone_rule():
    rule = PhoneRule()

    # Valid cases
    assert rule.validate("phone", "+1234567890", {}) is True

    # Invalid cases
    assert rule.validate("phone", "1234567890", {}) is False
    assert rule.validate("phone", "+123", {}) is False
    assert rule.validate("phone", None, {}) is False

    # Test message
    message = rule.default_message("phone", {})
    assert isinstance(message, str)
    assert "phone" in message


def test_slug_rule():
    rule = SlugRule()

    # Valid cases
    assert rule.validate("slug", "hello-world", {}) is True
    assert rule.validate("slug", "hello_world", {}) is True

    # Invalid cases
    assert rule.validate("slug", "hello world", {}) is False
    assert rule.validate("slug", None, {}) is False

    # Test message
    message = rule.message("slug", {})
    assert isinstance(message, str)
    assert "slug" in message
