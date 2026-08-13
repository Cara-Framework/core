"""Cast contract regression pins + ``ArrayCast`` NULL-preservation fix.

Five cast types — ``BoolCast``, ``IntCast``, ``FloatCast``,
``DecimalCast``, ``JsonCast`` — handle the bulk of the model
``__casts__`` declarations across the codebase. Each one has subtle
behaviour around three pivot inputs that production scrapers
historically wrote into the column wrong:

  * ``None`` (SQL NULL) — MUST survive both legs of the round-trip
    without collapsing to 0 / ``False`` / ``""`` / ``[]``. A
    ``brand_id`` cast as ``"integer"`` that collapses ``None`` → 0
    points at a non-existent row and trips the FK; an
    ``is_active`` boolean that collapses ``""`` to ``True`` flips
    rows on by accident.
  * ``""`` (empty string) — null-equivalent for nullable columns;
    must NOT round-trip as the literal ``""``/``"\\"\\""`` strings.
  * Type-mismatched input (string-shaped number, dict where a
    list is expected, etc.) — the cast either coerces predictably
    or rejects, but never silently corrupts on the write leg.

Most casts already handle these correctly. The two findings in this
file:

1. **Pin** the existing correct behaviour so a future "simplify"
   pass can't regress it. (``BoolCast``, ``IntCast``,
   ``DecimalCast``, ``JsonCast``.)

2. **Fix** ``ArrayCast.set(None)`` — pre-fix returned ``"[]"`` (the
   literal two-char JSON array string), so a nullable array column
   that the caller intended to write as SQL NULL stored a non-NULL
   ``"[]"`` instead. ``WHERE col IS NULL`` queries then missed
   every row the caller thought they had nulled, and the diff
   between ``col IS NULL`` and ``col = '[]'::jsonb`` quietly leaked
   into facet aggregation / sitemap filters.

   Read-side NULL also remains ``None``; a missing list is not an
   observed empty list. Non-list writes raise instead of silently
   discarding the caller's value into ``"[]"``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from cara.eloquent.casts.ArrayCast import ArrayCast
from cara.eloquent.casts.BoolCast import BoolCast
from cara.eloquent.casts.CollectionCast import CollectionCast
from cara.eloquent.casts.DecimalCast import DecimalCast
from cara.eloquent.casts.FloatCast import FloatCast
from cara.eloquent.casts.IntCast import IntCast
from cara.eloquent.casts.JsonCast import JsonCast

# ── BoolCast: comprehensive truthy / falsy / null pinning ────────


class TestBoolCast:
    """The cast historically translated ``bool("0")`` / ``bool("false")``
    to ``True`` because Python's ``bool(non_empty_string)`` is always
    True. The token sets fix the obvious cases; this test pins them so
    a "simplify back to ``bool(value)``" pass can't silently undo the
    fix for scrapers that write string-shaped truthiness into boolean
    columns."""

    @pytest.mark.parametrize(
        "token",
        [
            "true",
            "True",
            "TRUE",
            "1",
            " 1 ",
            "yes",
            "y",
            "t",
            "on",
        ],
    )
    def test_truthy_tokens(self, token):
        assert BoolCast().set(token) is True

    @pytest.mark.parametrize(
        "token",
        [
            "false",
            "False",
            "FALSE",
            "0",
            " 0 ",
            "no",
            "n",
            "f",
            "off",
            "",  # empty string is null-equivalent for bool intent
        ],
    )
    def test_falsy_tokens(self, token):
        result = BoolCast().set(token)
        assert result is False, (
            f"{token!r} must coerce to False; bool({token!r}) is True "
            "which used to flip nullable is_active columns on by accident"
        )

    def test_none_preserved(self):
        """SQL NULL stays NULL — a nullable boolean column must not
        silently flip to False/True just because the row is missing."""
        assert BoolCast().set(None) is None
        assert BoolCast().get(None) is None

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            (1, True),
            (0, False),
        ],
    )
    def test_native_types(self, value, expected):
        assert BoolCast().set(value) is expected
        assert BoolCast().get(value) is expected

    @pytest.mark.parametrize("value", [1.0, 0.0, 2, "truthy", [], {}])
    def test_unknown_shapes_stay_unknown(self, value):
        assert BoolCast().set(value) is None
        assert BoolCast().get(value) is None


# ── IntCast: None-preservation (FK columns) ──────────────────────


class TestIntCast:
    """``IntCast`` historically collapsed ``None`` → 0 which broke
    nullable FK columns: ``product_container.brand_id`` cast as
    ``integer`` would write 0 instead of NULL, and ``fk_product_brand_id``
    crashed because no Brand row has id=0. The fix preserves None as
    None on both legs; this pins it so a "simplify to int(value or 0)"
    refactor can't reintroduce the bug."""

    def test_none_preserved(self):
        assert IntCast().set(None) is None
        assert IntCast().get(None) is None

    def test_string_numbers_coerced(self):
        assert IntCast().set("42") == 42
        assert IntCast().set("  42  ") == 42

    def test_invalid_input_is_unknown_not_a_fabricated_zero(self):
        """An unparseable value must not become a valid foreign-key id."""
        assert IntCast().set("garbage") is None
        assert IntCast().get("garbage") is None

    @pytest.mark.parametrize("value", [True, False, 1.2, "1.2", "NaN", "Infinity"])
    def test_non_integral_or_non_finite_input_is_unknown(self, value):
        assert IntCast().set(value) is None


# ── FloatCast: None-preservation + invalid handling ──────────────


class TestFloatCast:
    def test_none_preserved(self):
        assert FloatCast().set(None) is None
        assert FloatCast().get(None) is None

    def test_invalid_input_is_unknown_not_a_measured_zero(self):
        assert FloatCast().set("not a number") is None

    def test_string_numbers_coerced(self):
        assert FloatCast().set("3.14") == 3.14

    @pytest.mark.parametrize("value", [True, False, "NaN", "Infinity", float("inf")])
    def test_non_finite_and_boolean_input_is_unknown(self, value):
        assert FloatCast().set(value) is None


# ── DecimalCast: precision quantisation ──────────────────────────


class TestDecimalCast:
    """The ``precision`` argument quantises both legs of the round-
    trip so the value persisted to a NUMERIC(_,2) column stays
    bit-for-bit identical to what the caller sees on read.

    Python's default Decimal rounding is ROUND_HALF_EVEN ("banker's
    rounding"): values exactly at the half mark round to the nearest
    even digit (1.005 → 1.00, 1.015 → 1.02). The test pins this so a
    future "switch to ROUND_HALF_UP for money" refactor surfaces here
    — every downstream comparison would have to be re-audited."""

    def test_none_preserved(self):
        assert DecimalCast(2).set(None) is None
        assert DecimalCast(2).get(None) is None

    def test_empty_string_treated_as_null(self):
        assert DecimalCast(2).set("") is None
        assert DecimalCast(2).set("   ") is None

    def test_quantises_to_requested_precision(self):
        assert DecimalCast(2).set("12.3456") == Decimal("12.35")
        assert DecimalCast(4).set("12.3456789") == Decimal("12.3457")

    def test_banker_s_rounding_at_half(self):
        """Python default = ROUND_HALF_EVEN. Pin the behaviour so any
        future swap to ROUND_HALF_UP is an intentional decision."""
        assert DecimalCast(2).set("1.005") == Decimal("1.00")
        assert DecimalCast(2).set("1.015") == Decimal("1.02")
        assert DecimalCast(2).set("1.025") == Decimal("1.02")
        assert DecimalCast(2).set("1.035") == Decimal("1.04")

    def test_invalid_returns_none(self):
        assert DecimalCast(2).set("not a number") is None

    @pytest.mark.parametrize("value", [True, False, float("nan"), "NaN", "Infinity"])
    def test_lossy_or_non_finite_input_is_unknown(self, value):
        assert DecimalCast(2).set(value) is None


# ── JsonCast: NULL vs "" vs valid JSON ──────────────────────────


class TestJsonCast:
    """JSON cast must distinguish SQL NULL (``None``) from
    null-equivalent empty strings, from the literal JSON-null
    (``"null"``). The fix that consolidated these pinned the
    contract — these tests stop a future tweak from quietly drifting."""

    def test_none_preserved(self):
        assert JsonCast().set(None) is None
        assert JsonCast().get(None) is None

    def test_empty_string_becomes_null(self):
        """Pre-fix ``set("")`` produced the literal JSON string
        ``'\"\"'``, and the next ``get()`` returned ``""`` — breaking
        ``if obj.field is None`` checks. The fix collapses empty
        strings to NULL on both legs."""
        assert JsonCast().set("") is None
        assert JsonCast().set("   ") is None
        assert JsonCast().get("") is None
        assert JsonCast().get("   ") is None

    def test_dict_roundtrip(self):
        original = {"key": "value", "n": 42, "list": [1, 2, 3]}
        encoded = JsonCast().set(original)
        assert JsonCast().get(encoded) == original

    def test_list_roundtrip(self):
        original = [1, "two", {"three": 3}]
        encoded = JsonCast().set(original)
        assert JsonCast().get(encoded) == original

    def test_valid_json_string_stored_as_is(self):
        """If the caller hands in a string that's already valid JSON,
        the cast persists it verbatim instead of re-encoding (avoids
        ``\\"key\\":\\"value\\"`` double-escaping). The next ``get()``
        parses it normally."""
        assert JsonCast().set('{"a": 1}') == '{"a": 1}'
        assert JsonCast().get('{"a": 1}') == {"a": 1}

    def test_invalid_json_string_encoded_as_literal(self):
        """A non-JSON string gets wrapped as a JSON string literal
        (``"not json"`` → ``'"not json"'``) so the round-trip is
        preserved even when the caller fed in plain text."""
        encoded = JsonCast().set("not json")
        assert JsonCast().get(encoded) == "not json"

    def test_unicode_preserved(self):
        """``ensure_ascii=False`` keeps Türkiye / Japanese / emoji
        chars intact in storage — without it the DB row stores
        ``\\u00e7`` escape sequences that double-encode on the next
        write cycle."""
        encoded = JsonCast().set({"city": "İstanbul", "emoji": "🔥"})
        assert "İstanbul" in encoded
        assert JsonCast().get(encoded) == {"city": "İstanbul", "emoji": "🔥"}

    def test_falsy_values_distinguished_from_null(self):
        """``0`` / ``False`` / ``[]`` / ``{}`` are valid JSON values
        that MUST round-trip as themselves — NOT silently coerce to
        None just because they're Python-falsy."""
        for value in (0, False, [], {}):
            encoded = JsonCast().set(value)
            assert encoded is not None, f"value={value!r} dropped to None"
            assert JsonCast().get(encoded) == value


# ── ArrayCast: NULL-preservation fix ────────────────────────────


class TestArrayCastNullPreservation:
    """Unknown stays NULL and malformed writes are rejected."""

    def test_set_none_returns_none_not_empty_array(self):
        """Pre-fix this returned ``"[]"`` — NULL drift for nullable
        columns. ``WHERE col IS NULL`` then missed every row written
        through this path."""
        assert ArrayCast().set(None) is None

    def test_get_none_preserves_unknown(self):
        assert ArrayCast().get(None) is None

    def test_set_empty_list_persists_as_empty_array(self):
        """An EXPLICIT empty list is distinct from ``None``: the
        caller wanted to write "empty array", not NULL. The cast
        must persist the JSON ``"[]"`` so a downstream
        ``where_raw("col = '[]'::jsonb")`` filter matches."""
        assert ArrayCast().set([]) == "[]"

    def test_set_populated_list_serialises_to_json(self):
        assert ArrayCast().set([1, 2, 3]) == "[1, 2, 3]"

    def test_set_non_list_is_rejected(self):
        with pytest.raises(TypeError, match="ArrayCast requires a list"):
            ArrayCast().set({"not": "a list"})


# ── CollectionCast: the same fix, in the same file, one cast later ──────


class TestCollectionCastNullPreservation:
    """CollectionCast shares ArrayCast's strict NULL/write contract."""

    def test_set_none_returns_none_not_empty_array(self):
        """Pre-fix this returned ``"[]"`` — the NULL drift its sibling
        cast's docstring says was already fixed."""
        assert CollectionCast().set(None) is None

    def test_get_none_preserves_unknown(self):
        assert CollectionCast().get(None) is None

    def test_set_empty_list_persists_as_empty_array(self):
        assert CollectionCast().set([]) == "[]"

    def test_set_populated_list_serialises_to_json(self):
        assert CollectionCast().set([1, 2, 3]) == "[1, 2, 3]"

    def test_set_collection_object_serialises_its_items(self):
        """The one input type this cast is named for.

        Pre-fix the unwrap probed for ``to_list``, a method cara's
        ``Collection`` has never published (it exposes ``all()`` /
        ``to_array()``), so a Collection fell through to the terminal
        ``return "[]"`` and the entire write was discarded — silently, since
        that branch carried no log either.
        """
        from cara.support.Collection import Collection

        assert CollectionCast().set(Collection([1, 2])) == "[1, 2]"

    def test_set_non_list_is_rejected(self):
        with pytest.raises(TypeError, match="CollectionCast requires a list"):
            CollectionCast().set({"not": "a list"})
