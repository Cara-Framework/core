"""``ViewEngine.escape_html`` — entity-presence heuristic XSS bug.

The template compiler wraps every ``{{ expression }}`` in
``escape(expression)`` (see ``ViewCompiler.py`` lines 235, 336) so
text-context interpolation is auto-escaped. ``escape`` resolves to
``ViewEngine.escape_html``. That function had a "skip if already
escaped" heuristic::

    if "&lt;" in value or "&gt;" in value or "&amp;" in value:
        return value

The intent was to avoid double-escaping pre-escaped content. The
effect was a partial-escape XSS: any string carrying ONE entity
(``"Sony &amp; Wireless"`` — extremely common in scraped product
titles) would skip the escape step for the WHOLE string. So a
title like ``"Sony &amp; Wireless <script>fetch('//x')</script>"``
flowed straight into the rendered HTML with the ``<script>`` tag
intact.

Real-world trigger surface: product-detail emails, deal alerts,
back-in-stock notifications, the digest — every template that
interpolates ``product.title`` against a scraped catalogue. ``&``
literals in ASIN titles round-trip through earlier sanitisers
as ``&amp;``; the script payload tucked behind one is what
escape_html was supposed to neutralise but didn't.

Fix: always escape, regardless of pre-existing entities. The
template author who genuinely wants raw HTML uses the ``raw()``
helper that the compiler already special-cases — that's the
single sanctioned escape hatch. The heuristic stays out of the
codebase entirely.

These tests pin the escape contract so a future "let's skip
double-escape" refactor surfaces here.
"""

from __future__ import annotations

import pytest

from cara.view.ViewEngine import ViewEngine


@pytest.fixture
def engine():
    """ViewEngine doesn't need any view paths to exercise ``escape_html``
    — it's a static text transform. Build a bare instance."""
    # ``ViewEngine`` may require some constructor args; build through
    # ``__new__`` and bind nothing — ``escape_html`` only touches
    # ``self`` for ``str(value)`` coercion, no state needed.
    return ViewEngine.__new__(ViewEngine)


# ── The bug: entity in input ≠ "already escaped" ──────────────────


class TestPartialEntityDoesNotSkipEscape:
    """A string that contains ``&amp;`` but ALSO contains raw ``<`` /
    ``>`` MUST still get its angle brackets escaped. Pre-fix the
    heuristic returned the whole string verbatim — the script tag
    rode in on the entity's coattails."""

    def test_amp_entity_does_not_pass_through_script_tag(self, engine):
        """The canonical regression case. ``&amp;`` appears legitimately
        in many scraped product titles (``"AT&T case"``, ``"Sons & Co."``)
        — the cast must not treat that as a green light to skip
        escaping the rest of the string."""
        hostile = "AT&amp;T <script>alert(1)</script>"
        out = engine.escape_html(hostile)
        # The script tag MUST be neutralised. After a real escape,
        # ``<script>`` becomes ``&lt;script&gt;``.
        assert "<script>" not in out, (
            f"escape_html let <script> through because an entity was "
            f"present; got: {out!r}"
        )
        assert "&lt;script&gt;" in out, (
            f"escape_html did not produce the entity-encoded form; got: {out!r}"
        )

    def test_lt_entity_does_not_pass_through_attribute_payload(self, engine):
        hostile = "Item rated 4&lt;5 stars <img src=x onerror=1>"
        out = engine.escape_html(hostile)
        assert "<img" not in out
        assert "&lt;img" in out

    def test_gt_entity_does_not_pass_through_javascript_url(self, engine):
        hostile = 'Range: 0 &gt; 99 <a href="javascript:alert(1)">x</a>'
        out = engine.escape_html(hostile)
        # The angle brackets on the <a> tag must be entity-encoded;
        # ``javascript:`` itself isn't dangerous as plain text but
        # would activate as a link target if the <a> rendered live.
        assert "<a " not in out
        assert "&lt;a " in out


# ── Round-trip semantics: already-escaped content shouldn't be
#    double-escaped by the helper — but the safe way to prevent that
#    is at the call site, NOT by trusting an entity-presence heuristic.


class TestDoubleEscapeIsAcceptable:
    """If the caller passes content that's already entity-encoded,
    re-running ``escape_html`` turns ``&amp;`` into ``&amp;amp;``.
    That's the documented contract — callers who want raw output use
    the ``raw()`` template helper. Re-encoding is strictly safer
    than the pre-fix "skip on entity" path."""

    def test_already_escaped_amp_re_escapes(self, engine):
        out = engine.escape_html("Sons &amp; Co.")
        # Pre-fix this passed through verbatim. Post-fix the ``&`` in
        # ``&amp;`` re-encodes — the user-visible string in the
        # rendered HTML is the same (``Sons &amp; Co.``) because the
        # browser decodes one level. The defensive double-encode is
        # safe; the underdone single-encode wasn't.
        assert out == "Sons &amp;amp; Co."

    def test_plain_text_round_trips(self, engine):
        """Sanity: a string with no special chars MUST pass through
        unchanged. The fix tightens the over-eager skip; it must
        not over-encode harmless text."""
        out = engine.escape_html("Hello world 42")
        assert out == "Hello world 42"


# ── None / non-string ────────────────────────────────────────────


class TestNonStringHandling:
    def test_none_returns_empty_string(self, engine):
        """``None`` in a template binding is a missing key; render as
        empty rather than the string ``"None"`` which would leak the
        absence into the user-visible HTML."""
        assert engine.escape_html(None) == ""

    def test_int_coerced_to_string(self, engine):
        assert engine.escape_html(42) == "42"

    def test_bool_coerced_to_string(self, engine):
        assert engine.escape_html(True) == "True"


# ── Full escape table ────────────────────────────────────────────


class TestFullEscapeTable:
    """All five HTML-significant characters MUST be encoded. Pin the
    exact entities — a future refactor that swaps ``&#x27;`` for
    ``&apos;`` (which Internet Explorer ≤ 8 doesn't render) would
    surface here."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("&", "&amp;"),
            ("<", "&lt;"),
            (">", "&gt;"),
            ('"', "&quot;"),
            ("'", "&#x27;"),
        ],
    )
    def test_each_char(self, engine, raw, expected):
        assert engine.escape_html(raw) == expected

    def test_combined(self, engine):
        assert (
            engine.escape_html("<a href=\"x\" data-y='z'>&lt;</a>")
            == "&lt;a href=&quot;x&quot; data-y=&#x27;z&#x27;&gt;&amp;lt;&lt;/a&gt;"
        )
