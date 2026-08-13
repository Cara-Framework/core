"""ReturnPath — the open-redirect guard for caller-supplied destinations.

The value under test reaches production by being written into a URL the
product itself emails out, so a bypass here turns the product's own domain
into a phishing hop. Each case below is a real-world bypass shape, not a
hypothetical.
"""

import pytest

from cara.support import ReturnPath


@pytest.mark.parametrize(
    "value",
    [
        "/overview",
        "/listings/123",
        "/search?q=blue+widget&page=2",
        "/catalog/products/1#media",
        "/",
    ],
)
def test_internal_paths_are_accepted(value):
    assert ReturnPath.is_safe(value) is True
    assert ReturnPath.safe(value) == value


@pytest.mark.parametrize(
    ("value", "why"),
    [
        ("https://evil.example/login", "absolute URL"),
        ("//evil.example/login", "protocol-relative"),
        ("/\\evil.example", "backslash reaches the authority position"),
        ("/\\/evil.example", "mixed slash/backslash authority"),
        ("/legit\\..\\evil", "backslash anywhere"),
        ("javascript:alert(1)", "non-path scheme"),
        ("overview", "relative, not rooted"),
        ("/\tevil.example", "control character a parser may strip"),
        ("/\nSet-Cookie: x=1", "newline injection"),
        ("", "empty"),
        (None, "absent"),
        (42, "not a string"),
    ],
)
def test_hostile_shapes_are_refused(value, why):
    assert ReturnPath.is_safe(value) is False, why


def test_percent_encoded_authority_is_refused():
    """`/%2F%2Fevil` looks internal until something decodes it once."""
    assert ReturnPath.is_safe("/%2F%2Fevil.example") is False
    assert ReturnPath.is_safe("/%2Fevil.example") is False


def test_overlong_paths_are_refused():
    assert ReturnPath.is_safe("/" + "a" * ReturnPath.MAX_LENGTH) is False


def test_safe_falls_back_instead_of_raising():
    """A hostile path is routine input on a public endpoint. The caller's
    real work — send the mail, finish the sign-in — must still happen."""
    assert ReturnPath.safe("//evil.example", "/overview") == "/overview"
    assert ReturnPath.safe(None) == "/"
    assert ReturnPath.safe("//evil.example", "") == ""
