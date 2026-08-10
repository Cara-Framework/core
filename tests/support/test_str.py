from cara.support.Str import slugify


def test_slugify():
    assert slugify("Hello World") == "hello-world"
    assert slugify("  ") == ""
    assert slugify("Héllo Wörld") == "hello-world"
    assert slugify("Hello World", "_") == "hello_world"


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


def test_slugify_folds_accents_outside_the_explicit_map():
    """An accent must FOLD to its ASCII base, never be deleted.

    Deletion corrupts tokens ("Nestlé" → ``nestl``), which both splits one
    entity across two slugs and lets distinct stems collide once the accent
    is thrown away. The explicit char_map covers Turkish and the common
    Latin-1 vowels; everything else reaches the NFKD fold.
    """
    assert slugify("Škoda") == "skoda"
    assert slugify("Žilina") == "zilina"
    assert slugify("Nestlé") == "nestle"
    assert slugify("Müller") == "muller"
    assert slugify("Curaçao") == "curacao"
    assert slugify("Ångström") == "angstrom"


def test_slugify_is_unicode_normalization_form_insensitive():
    """The same text in NFC and NFD must produce the SAME slug."""
    nfc = "caf\u00e9"  # é as ONE code point
    nfd = "cafe\u0301"  # e + combining acute
    assert nfc != nfd
    assert slugify(nfc) == slugify(nfd) == "cafe"


def test_slugify_distinct_accented_stems_stay_distinct():
    """Folding must not merge letters that differ in their BASE."""
    assert slugify("Máller") == "maller"
    assert slugify("Müller") == "muller"
