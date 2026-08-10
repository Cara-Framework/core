"""GTIN normalization — one barcode, one canonical key, no junk identities.

These tests pin the decisions the two normalizers that predated this module
disagreed on: whether zero-padding matters, whether a placeholder barcode is
an identity, and how short an input may be before it stops being a barcode.
"""

from __future__ import annotations

import pytest

from cara.support import (
    MIN_GTIN_DIGITS,
    coerce_to_gtin_14,
    gtin_check_digit,
    is_valid_gtin,
    normalize_gtin,
    normalize_isbn,
)


class TestCheckDigit:
    def test_computes_the_gs1_mod_10_digit(self):
        # 03600029145 + check 2 == the UPC-A on a real retail item.
        assert gtin_check_digit("03600029145") == 2
        assert gtin_check_digit("01234567890") == 5

    def test_left_zero_padding_cannot_change_the_check_digit(self):
        # Padding adds zeros in the high positions; they contribute 0 to
        # the weighted sum whatever weight they land on. This is WHY every
        # padding of one barcode validates alike.
        assert gtin_check_digit("03600029145") == gtin_check_digit("0003600029145")

    def test_rejects_non_digits(self):
        assert gtin_check_digit("") is None
        assert gtin_check_digit("12a45") is None


class TestPaddingEquivalence:
    def test_upc_ean_and_gtin_forms_of_one_barcode_collapse(self):
        upc = normalize_gtin("036000291452")
        ean = normalize_gtin("0036000291452")
        gtin = normalize_gtin("00036000291452")
        assert upc == ean == gtin == "00036000291452"

    def test_separators_are_ignored(self):
        assert normalize_gtin(" 0-36000-29145-2 ") == "00036000291452"

    def test_over_padded_input_still_normalizes(self):
        # More than 14 raw digits is fine when the excess is leading zeros:
        # the value is still one GTIN, just written with extra padding.
        assert normalize_gtin("000000036000291452") == "00036000291452"

    def test_more_than_fourteen_significant_digits_is_not_a_gtin(self):
        assert normalize_gtin("123456789012345") is None

    def test_bad_check_digit_is_rejected(self):
        assert normalize_gtin("036000291453") is None

    def test_non_numeric_input_is_rejected(self):
        assert normalize_gtin("not-a-barcode") is None
        assert normalize_gtin(None) is None
        assert normalize_gtin("") is None


class TestLengthFloor:
    """Fewer than 8 digits is not a barcode — padding is recognised, never invented."""

    def test_min_length_is_the_shortest_standard_form(self):
        assert MIN_GTIN_DIGITS == 8

    def test_short_numeric_junk_never_mints_an_identity(self):
        # "48" zero-padded to a GTIN-8 ("00000048") passes Mod-10 by
        # accident. Accepting it would give two unrelated records whose
        # only "barcode" is the number 48 the SAME match key.
        assert gtin_check_digit("0000004") == 8  # the accident is real
        assert normalize_gtin("48") is None
        assert normalize_gtin("1234567") is None

    def test_eight_digits_is_accepted(self):
        assert normalize_gtin("96385074") == "00000096385074"


class TestPlaceholderRejection:
    @pytest.mark.parametrize(
        "raw",
        [
            "00000000",
            "000000000000",
            "0000000000000",
            "00000000000000",
            "11111111",
            "99999999999999",
        ],
    )
    def test_all_same_digit_values_are_not_identities(self, raw):
        assert normalize_gtin(raw) is None

    @pytest.mark.parametrize("raw", ["0123456789012", "00987654321098", "12345678901231"])
    def test_known_placeholder_barcodes_are_rejected(self, raw):
        # These verify Mod-10 — only the name list keeps them out.
        assert is_valid_gtin(raw) is True
        assert normalize_gtin(raw) is None

    @pytest.mark.parametrize("raw", ["2345678901234", "34567890", "21098765"])
    def test_consecutive_digit_runs_are_rejected(self, raw):
        # A run of consecutive digits (either direction, wrapping at 9→0) is
        # what a source emits when it has no identifier; it is never a real
        # allocation, and it verifies Mod-10 often enough to matter.
        assert is_valid_gtin(raw) is True
        assert normalize_gtin(raw) is None

    def test_a_run_that_breaks_is_a_real_barcode(self):
        # The rule keys on the WHOLE significant run, not a prefix of one:
        # "12345678905" walks 1..9,0 then jumps to 5, so it is kept.
        assert normalize_gtin("12345678905") == "00012345678905"

    def test_a_real_barcode_with_repeated_digits_survives(self):
        assert normalize_gtin("30194253297131") == "30194253297131"


class TestIsValidGtin:
    @pytest.mark.parametrize(
        "raw", ["96385074", "036000291452", "0036000291452", "00036000291452"]
    )
    def test_accepts_every_canonical_length(self, raw):
        assert is_valid_gtin(raw) is True

    @pytest.mark.parametrize("raw", ["1234567", "123456789", "036000291453", ""])
    def test_rejects_wrong_length_or_wrong_check_digit(self, raw):
        assert is_valid_gtin(raw) is False

    def test_placeholder_values_are_structurally_valid(self):
        # is_valid_gtin answers "is this a well-formed GTIN", NOT "is this a
        # real identity" — placeholder rejection is normalize_gtin's job.
        assert is_valid_gtin("00000000000000") is True


class TestIsbn:
    def test_isbn10_upgrades_to_isbn13(self):
        assert normalize_isbn("0201530821") == "9780201530827"

    def test_isbn10_with_x_check_character(self):
        assert normalize_isbn("030640615X") == "9780306406157"

    def test_hyphenated_and_bare_forms_agree(self):
        assert normalize_isbn("0-201-53082-1") == normalize_isbn("0201530821")

    def test_isbn13_passes_through(self):
        assert normalize_isbn("9780306406157") == "9780306406157"
        assert normalize_isbn("978-0-306-40615-7") == "9780306406157"

    def test_zero_padded_isbn13_loses_the_padding(self):
        assert normalize_isbn("09780306406157") == "9780306406157"

    def test_invalid_inputs_return_none(self):
        assert normalize_isbn("9780306406159") is None
        assert normalize_isbn("12345678901") is None
        assert normalize_isbn(None) is None
        assert normalize_isbn("") is None


class TestCoerce:
    def test_prefers_the_base_consumer_unit(self):
        # Indicator digit 0 is the single retail item; 3 is a case of them.
        assert (
            coerce_to_gtin_14(gtin="30194253297131", upc="036000291452")
            == "00036000291452"
        )

    def test_falls_back_to_the_only_candidate(self):
        assert coerce_to_gtin_14(gtin="30194253297131") == "30194253297131"

    def test_isbn_folds_into_the_same_key_space(self):
        from_isbn10 = coerce_to_gtin_14(isbn="0375760393")
        from_isbn13 = coerce_to_gtin_14(isbn="9780375760396")
        from_ean = coerce_to_gtin_14(ean="9780375760396")
        assert from_isbn10 == from_isbn13 == from_ean == "09780375760396"

    def test_an_explicit_barcode_outranks_an_isbn(self):
        assert (
            coerce_to_gtin_14(upc="190198001443", isbn="9780375760396")
            == "00190198001443"
        )

    def test_all_placeholder_inputs_yield_no_key(self):
        assert coerce_to_gtin_14(gtin="00000000000000", upc="000000000000") is None

    def test_no_usable_input_returns_none(self):
        assert coerce_to_gtin_14() is None
        assert coerce_to_gtin_14(gtin="", upc=None, ean="junk", isbn="123") is None
