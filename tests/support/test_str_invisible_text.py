"""Names made of nothing visible, or that reorder what is shown, never reach
storage (qa18): ``strip_invisible`` drops Unicode format characters and blank
look-alikes, ``clean_line`` folds a single-line field onto one line, and
``sanitize_text`` no longer keeps either."""

from __future__ import annotations

import pytest

from cara.support import clean_line, sanitize_text, strip_invisible


@pytest.mark.parametrize(
    "raw",
    [
        "\u200b\u200b\u200b",  # zero-width spaces
        "\u202e\u2066\u2069",  # bidi override + isolates
        "\ufeff\u00ad\u2060",  # BOM, soft hyphen, word joiner
        "\u3164\u3164\u3164",  # Hangul filler
        "\u2800\u2800",  # braille blank
        "\u200d\u200d\u200d",  # bare joiners
    ],
)
def test_an_invisible_name_cleans_to_nothing(raw):
    assert strip_invisible(raw) == ""
    assert clean_line(raw) == ""
    assert sanitize_text(raw) == ""


def test_a_bidi_override_cannot_reverse_a_name():
    assert clean_line("Acme\u202etxt.exe") == "Acmetxt.exe"
    assert sanitize_text("Acme\u202etxt.exe") == "Acmetxt.exe"


def test_joiners_inside_emoji_and_scripts_survive():
    family = "\U0001f469\u200d\U0001f4bb"  # 👩\u200d💻
    heart_on_fire = "❤️\u200d\U0001f525"  # ❤️\u200d🔥
    persian = "می\u200cخواهم"  # می\u200cخواهم
    scotland = "\U0001f3f4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f"
    for text in (family, heart_on_fire, persian, scotland):
        assert strip_invisible(text) == text
    # A joiner between Latin letters joins nothing visible.
    assert strip_invisible("Ad\u200dmin") == "Admin"


@pytest.mark.parametrize(
    ("raw", "line"),
    [
        ("Acme\r\nOutdoors", "Acme Outdoors"),
        ("Acme\nInjected: header", "Acme Injected: header"),
        ("Acme Co Ltd\x85X", "Acme Co Ltd X"),
        ("  Acme\t\t Co  ", "Acme Co"),
        ("Acme\x00\x07Co", "Acme Co"),
        ("Ａｃｍｅ", "Acme"),  # fullwidth → NFKC
    ],
)
def test_clean_line_folds_every_break_onto_one_line(raw, line):
    assert clean_line(raw) == line


def test_clean_line_truncates_after_cleaning():
    assert clean_line("\u200b" * 50 + "abcdef", max_length=3) == "abc"
    assert clean_line(None) == ""


def test_sanitize_text_keeps_its_paragraphs():
    assert sanitize_text("one\n\n\n\ntwo\u200b") == "one\n\ntwo"
