import pytest

from cara.cache import glob_literal_head, patterns_within_families

FAMILIES = ("notifications:", "home:")


def test_literal_head_stops_at_first_glob_meta():
    assert glob_literal_head("home:*:v2") == "home:"
    assert glob_literal_head("a?b") == "a"
    assert glob_literal_head("x[ab]") == "x"
    assert glob_literal_head("plain") == "plain"


def test_accepts_and_dedupes_inside_families():
    out = patterns_within_families([" home:* ", "home:*", "notifications:1:*"], FAMILIES)
    assert out == ["home:*", "notifications:1:*"]


@pytest.mark.parametrize("bad", ["*", "h*", "auth:*", "home\\:*", "[h]ome:*"])
def test_refuses_patterns_escaping_families(bad):
    with pytest.raises(ValueError):
        patterns_within_families([bad], FAMILIES)


@pytest.mark.parametrize("items", [[], None, [""], ["  "], [3]])
def test_refuses_empty_or_non_string(items):
    with pytest.raises(ValueError):
        patterns_within_families(items, FAMILIES)


def test_refuses_empty_allowlist():
    with pytest.raises(ValueError):
        patterns_within_families(["home:*"], ())
