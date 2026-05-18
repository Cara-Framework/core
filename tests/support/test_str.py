import pytest

from cara.support.Str import (
    camel_case,
    format_money_cents,
    kebab_case,
    normalize_email,
    slugify,
    snake_case,
    title_case,
    truncate,
)


def test_slugify():
    assert slugify("Hello World") == "hello-world"
    assert slugify("  ") == ""
    assert slugify("Héllo Wörld") == "hello-world"
    assert slugify("Hello World", "_") == "hello_world"


def test_normalize_email():
    assert normalize_email(" Alice@Example.COM ") == "alice@example.com"
    assert normalize_email("") == ""
    assert normalize_email("  ") == ""


def test_normalize_email_edge_cases():
    from cara.support.Str import normalize_email
    # None input
    assert normalize_email(None) == ""
    # tabs and mixed whitespace
    assert normalize_email("\t Bob@Example.COM \n") == "bob@example.com"
    # already normalized
    assert normalize_email("alice@example.com") == "alice@example.com"


def test_format_money():
    assert format_money_cents(1050) == "$10.50"
    assert format_money_cents(0) == "$0.00"
    assert format_money_cents(100000, "EUR") == "€1,000.00"

    with pytest.raises(TypeError):
        format_money_cents("1050")

    with pytest.raises(ValueError):
        format_money_cents(-100)


def test_truncate():
    assert truncate("Hello World", 5) == "Hello..."
    assert truncate("Hi", 10) == "Hi"
    assert truncate("Hello", 3, "!") == "Hel!"
    assert truncate("", 5) == ""


def test_title_case():
    assert title_case("hello world") == "Hello World"
    assert title_case("hello-world") == "Hello World"
    assert title_case("hello_world") == "Hello World"


def test_title_case_edge_cases():
    assert title_case("  foo__bar--baz  ") == "Foo Bar Baz"
    assert title_case("___hello___") == "Hello"
    assert title_case("---") == ""
    assert title_case("   ") == ""
    assert title_case("hello_WORLD") == "Hello World"


def test_snake_case():
    assert snake_case("Hello World") == "hello_world"
    assert snake_case("helloWorld") == "hello_world"
    assert snake_case("kebab-case") == "kebab_case"


def test_kebab_case():
    assert kebab_case("Hello World") == "hello-world"
    assert kebab_case("helloWorld") == "hello-world"
    assert kebab_case("snake_case") == "snake-case"


def test_camel_case():
    assert camel_case("hello world") == "helloWorld"
    assert camel_case("Hello World") == "helloWorld"
    assert camel_case("snake_case") == "snakeCase"


def test_snake_case_edge_cases():
    from cara.support.Str import snake_case
    # acronyms: snake_case only splits lower-to-upper transitions, so
    # consecutive uppercase letters stay glued together
    assert snake_case("HTTPServer") == "httpserver"
    assert snake_case("getHTTPResponse") == "get_httpresponse"
    # consecutive non-alphanumeric characters collapse to a single underscore
    assert snake_case("foo--bar__baz") == "foo_bar_baz"
    assert snake_case("  foo  bar  ") == "foo_bar"
    # digit boundaries: digits are alphanumeric, so a letter/number run stays together
    assert snake_case("user123name") == "user123name"
    assert snake_case("item2Value") == "item2_value"
    # empty / None-ish input
    assert snake_case("") == ""
    assert snake_case(None) == ""


def test_kebab_case_edge_cases():
    from cara.support.Str import kebab_case
    assert kebab_case("HTTPServer") == "httpserver"
    assert kebab_case("getHTTPResponse") == "get-httpresponse"
    assert kebab_case("foo--bar__baz") == "foo-bar-baz"
    assert kebab_case("  foo  bar  ") == "foo-bar"
    assert kebab_case("item2Value") == "item2-value"
    assert kebab_case("") == ""
    assert kebab_case(None) == ""


def test_camel_case_edge_cases():
    from cara.support.Str import camel_case
    # underscores, hyphens, and spaces are all valid separators
    assert camel_case("hello_world") == "helloWorld"
    assert camel_case("hello-world") == "helloWorld"
    assert camel_case("hello world") == "helloWorld"
    # multiple separators and surrounding whitespace
    assert camel_case("  foo__bar--baz  ") == "fooBarBaz"
    # single-word input: first word is fully lowercased
    assert camel_case("Hello") == "hello"
    assert camel_case("HELLO") == "hello"
    # empty / None-ish input
    assert camel_case("") == ""
    assert camel_case(None) == ""


def test_pluralize():
    from cara.support.Str import pluralize
    assert pluralize("cat") == "cats"
    assert pluralize("box") == "boxes"
    assert pluralize("bush") == "bushes"
    assert pluralize("church") == "churches"
    assert pluralize("baby") == "babies"
    assert pluralize("day") == "days"
    assert pluralize("hero") == "heroes"
    assert pluralize("radio") == "radios"
    assert pluralize("") == ""
    assert pluralize(None) == ""


def test_studly_case():
    from cara.support.Str import studly_case
    assert studly_case("foo_bar") == "FooBar"
    assert studly_case("hello-world") == "HelloWorld"
    assert studly_case("  foo__bar--baz  ") == "FooBarBaz"
    assert studly_case("") == ""
    assert studly_case("   ") == ""
    assert studly_case("hello_WORLD") == "HelloWorld"


def test_slugify_edge_cases():
    assert slugify("") == ""
    assert slugify(None) == ""
    assert slugify("hello") == "hello"
    assert slugify("HELLO WORLD") == "hello-world"
    assert slugify("--hello--world--") == "hello-world"
    assert slugify("caf\xe9 r\xe9sum\xe9") == "cafe-resume"
    assert slugify("\u0130stanbul \u015eehri") == "istanbul-sehri"
    assert slugify("foo   bar") == "foo-bar"
    assert slugify("hello world", ".") == "hello.world"
    assert slugify("a&b=c") == "a-b-c"


def test_slugify_max_length_caps_at_word_boundary():
    """``max_length`` truncates at the last separator before the cap so the
    cut lands on a word boundary, not mid-word. Without this, a slug
    truncated mid-token leaks the cut point and looks broken to users."""
    raw = "the quick brown fox jumps over the lazy dog"
    out = slugify(raw, max_length=20)
    # 20-char cap; last separator before pos 20 is between "brown" and
    # "fox" (``the-quick-brown-fox`` is 19 chars, the next ``-`` would
    # push it to 20). Implementation truncates to the last full word.
    assert len(out) <= 20
    assert not out.endswith("-")
    # Cut lands on a word boundary \u2014 the head is a real word, not a fragment.
    assert out in (
        "the-quick-brown-fox",
        "the-quick-brown",
        "the-quick",
    )


def test_slugify_max_length_hard_slice_when_no_separator():
    """When the input is a single very long token with no separators in
    the head, fall back to a hard slice \u2014 better than returning ``""``."""
    out = slugify("a" * 500, max_length=80)
    assert out == "a" * 80


def test_slugify_max_length_no_effect_when_within_cap():
    """The cap should be a no-op for inputs already under the limit."""
    assert slugify("hello world", max_length=255) == "hello-world"


def test_slugify_max_length_none_disables_cap():
    """``max_length=None`` (default) preserves the historical behaviour
    of returning the full-length slug \u2014 callers persisting to a bounded
    column must opt in explicitly."""
    huge = "word " * 200
    out = slugify(huge)
    assert len(out) > 255
    assert slugify(huge, max_length=None) == out


def test_slugify_non_latin_returns_empty_today():
    """Pinned behaviour: non-Latin / emoji-only inputs strip to ``""``.
    Callers that need a guaranteed-non-empty slug (the product
    consolidator) must layer a fallback on top. Documents the contract
    so a future "transliterate all scripts" change doesn't break
    fallback expectations silently."""
    assert slugify("\u30c6\u30ec\u30d3") == ""
    assert slugify("\ud83d\udcf1\ud83d\udcf1\ud83d\udcf1") == ""
    assert slugify("\u7535\u89c6") == ""
    assert slugify("\u041a\u043d\u0438\u0433\u0430") == ""


def test_format_money_edge_cases():
    assert format_money_cents(1) == "$0.01"
    assert format_money_cents(99) == "$0.99"
    assert format_money_cents(100) == "$1.00"
    assert format_money_cents(1234567, "GBP") == "\u00a312,345.67"
    assert format_money_cents(0, "TRY") == "\u20ba0.00"
    assert format_money_cents(50, "AUD") == "A$0.50"
    assert format_money_cents(50, "CAD") == "C$0.50"
    with pytest.raises(ValueError):
        format_money_cents(100, "JPY")
    with pytest.raises(TypeError):
        format_money_cents(10.5)
    with pytest.raises(TypeError):
        format_money_cents(None)


def test_truncate_edge_cases():
    assert truncate(None, 5) == ""
    assert truncate("Hello", 5) == "Hello"
    assert truncate("Hello", 0) == "..."
    assert truncate("Hello", 5, "") == "Hello"
    assert truncate("Hello World", 5, "") == "Hello"
