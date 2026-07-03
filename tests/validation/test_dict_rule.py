from cara.validation.rules.DictRule import DictRule


def test_dict_rule_accepts_mappings():
    rule = DictRule()
    assert rule.validate("settings", {}, {}) is True
    assert rule.validate("settings", {"a": 1}, {}) is True


def test_dict_rule_rejects_non_mappings():
    rule = DictRule()
    assert rule.validate("settings", [], {}) is False
    assert rule.validate("settings", "{}", {}) is False  # encoded string is `json`'s job
    assert rule.validate("settings", 3, {}) is False
    assert rule.validate("settings", None, {}) is False  # nullable short-circuits first


def test_dict_rule_message():
    message = DictRule().default_message("settings", {})
    assert "settings" in message and "object" in message
