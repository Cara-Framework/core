"""``strip_tags`` / ``sanitize_text`` leave no live tag behind, in linear time,
and ``redact_log_secrets`` covers the shapes a secret is actually logged in.

Decoding entities was the LAST step of ``strip_tags`` (and the NFKC fold came
after it in ``sanitize_text``), so encoded markup went in as text and came out
as a tag. The tag regexes rescanned to the end of the text from every
unclosed ``<``. A quoted secret holding a space or ``,;&#}`` was not redacted
at all.
"""

from __future__ import annotations

import time

import pytest

from cara.support import redact_log_secrets, sanitize_text, strip_tags


@pytest.mark.parametrize(
    "raw",
    [
        "&lt;script&gt;alert(1)&lt;/script&gt;",
        "&#60;svg onload=alert(1)&#62;",
        "&#x3c;img src=x onerror=alert(1)&#x3e;",
        "&ltscript&gtalert(1)&lt/script&gt",  # legacy entities without ';'
        "&amp;lt;script&amp;gt;alert(1)&amp;lt;/script&amp;gt;",  # double-encoded
        "＜script＞alert(1)＜/script＞",  # fullwidth brackets
        "&#xFF1C;script&#xFF1E;alert(1)",  # encoded fullwidth brackets
        "&lt;&#x200b;script&gt;alert(1)",  # zero-width char inside the tag
    ],
)
def test_sanitize_text_never_returns_a_tag_it_decoded(raw):
    cleaned = sanitize_text(raw)

    assert "<" not in cleaned
    assert ">" not in cleaned


def test_strip_tags_strips_the_tags_its_own_decoding_exposes():
    assert strip_tags("&lt;img src=x onerror=alert(1)&gt;hi") == "hi"
    assert strip_tags("&lt;script&gt;alert(1)&lt;/script&gt;ok") == "ok"
    assert strip_tags("&amp;lt;b&amp;gt;bold&amp;lt;/b&amp;gt;") == "bold"


def test_decoded_prose_with_comparisons_keeps_its_middle():
    # Only what a browser reads as a tag is stripped from DECODED text.
    assert (
        strip_tags("Size &lt; 10cm &amp; weight &gt; 2kg") == "Size < 10cm & weight > 2kg"
    )
    assert sanitize_text("Tom &amp; Jerry") == "Tom & Jerry"
    assert sanitize_text("1 &lt; 2") == "1 < 2"


def test_typed_markup_is_stripped_exactly_as_before():
    assert strip_tags("a<script>1</script>b<style>2</style>c") == "abc"
    assert strip_tags("<SCRIPT>x</script >after") == "after"
    assert strip_tags("I <3 you <b>bold</b>") == "I bold"
    assert strip_tags("<<b>script>alert(1)</<b>script>") == "script>alert(1)script>"
    # The leftover ``<script b`` is an unterminated tag: dropped to the end.
    assert strip_tags("<script <style>a</style>b") == ""
    assert strip_tags("1 < 2") == "1 < 2"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("hello <img src=x onerror=alert(1)", "hello "),
        ("<b>ok</b> then <svg onload=alert(1)", "ok then "),
        ("text </div", "text "),
        ("x <!-- open comment", "x "),
        ("hi &lt;img src=x onerror=alert(1)", "hi "),  # exposed by decoding
    ],
)
def test_an_unterminated_tag_at_the_end_is_dropped(raw, expected):
    # The page's own next '>' would close it once embedded.
    assert strip_tags(raw) == expected
    assert "<" not in sanitize_text(raw)


@pytest.mark.parametrize("raw", ["a < b", "I <3 you", "x <", "1 <= 2", "<<"])
def test_a_bracket_that_opens_no_tag_stays_text(raw):
    assert strip_tags(raw) == raw
    assert sanitize_text(raw) == raw


def test_entities_nested_past_the_pass_cap_stay_inert():
    deep = "&" + "amp;" * 50 + "lt;script&gt;alert(1)"

    cleaned = sanitize_text(deep)

    assert "<" not in cleaned


@pytest.mark.parametrize(
    "unit",
    [
        "<",  # unclosed bracket
        "<script>",  # opener with no closer
        "<script ",  # opener nested in one unclosed tag
        "</script ",  # closer with a whitespace run and no '>'
    ],
)
def test_unclosed_markup_is_stripped_in_linear_time(unit):
    # 200k repeats took minutes with the old regexes (80k '<' alone: 2 s).
    text = unit * 200_000 + (">" if unit == "<script " else "")

    started = time.perf_counter()
    strip_tags(text)

    assert time.perf_counter() - started < 2.0


def test_entity_bomb_costs_a_bounded_number_of_passes():
    text = "&" + "amp;" * 100_000 + "lt;script&gt;"

    started = time.perf_counter()
    sanitize_text(text)

    assert time.perf_counter() - started < 2.0


@pytest.mark.parametrize(
    "message, secret",
    [
        ('{"password": "correct horse battery"}', "horse"),
        ('{"password": "p@ss,word!"}', "p@ss"),
        ("{'client_secret': 'a;b&c#d}e'}", "a;b"),
        ('"token": "ab\\"cd ef" trailing', "cd ef"),
        ('"api_key": "cut off mid-line', "cut off"),
        ("{'Authorization': 'Bearer eyJ.payload.sig'}", "eyJ.payload.sig"),
        ('{"Proxy-Authorization": "Basic dXNlcjpwYXNz"}', "dXNlcjpwYXNz"),
        ("could not connect to postgresql://app:s3cr3t@db:5432/app", "s3cr3t"),
        ("Error 111 connecting to redis://:hunter22@cache:6379/0", "hunter22"),
        ("amqp://worker:p4ss@broker:5672/%2F unreachable", "p4ss"),
    ],
)
def test_logged_secret_shapes_are_redacted(message, secret):
    redacted = redact_log_secrets(message)

    assert secret not in redacted
    assert "[REDACTED]" in redacted


def test_redaction_keeps_the_surrounding_structure():
    assert (
        redact_log_secrets('{"password": "correct horse", "user": "ada"}')
        == '{"password": "[REDACTED]", "user": "ada"}'
    )
    assert (
        redact_log_secrets("{'Authorization': 'Bearer abc.def', 'Accept': '*/*'}")
        == "{'Authorization': 'Bearer [REDACTED]', 'Accept': '*/*'}"
    )
    assert (
        redact_log_secrets("postgresql://app:pw@db:5432/app")
        == "postgresql://[REDACTED]@db:5432/app"
    )
    # No userinfo: an '@' in the query string is not a credential.
    assert (
        redact_log_secrets("GET https://example.test/a?email=a@b.test")
        == "GET https://example.test/a?email=a@b.test"
    )


def test_redaction_of_a_long_token_is_linear():
    started = time.perf_counter()
    redact_log_secrets("a" * 2_000_000)

    assert time.perf_counter() - started < 2.0
