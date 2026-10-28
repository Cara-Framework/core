"""``present`` resolves wildcard-expanded paths through list indexes."""

from __future__ import annotations

from cara.validation.rules.PresentRule import PresentRule


def _present(data: dict, field: str) -> bool:
    return PresentRule().validate(field, None, {"_data": data})


def test_present_follows_list_indexes() -> None:
    data = {"conditions": {"rules": [{"value": "x"}, {"value": None}]}}
    assert _present(data, "conditions.rules.0.value")
    assert _present(data, "conditions.rules.1.value")


def test_present_still_refuses_absent_keys() -> None:
    data = {"conditions": {"rules": [{"field": "a"}]}}
    assert not _present(data, "conditions.rules.0.value")
    assert not _present(data, "conditions.rules.3.value")
    assert _present({"user": {"email": ""}}, "user.email")
