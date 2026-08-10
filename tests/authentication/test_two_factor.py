"""TOTP primitives against the RFC's own numbers.

The point of pinning RFC 4226 Appendix D and RFC 6238 Appendix B here is that
a hand-rolled OTP is only worth anything if it agrees with every
authenticator app in the world. A unit test written against our own output
would happily certify a subtly wrong dynamic-truncation offset; these
vectors will not.

RFC 6238's published values are 8 digits and this implementation emits 6, so
each expected code below is the RFC value's last six digits — that is what
truncating to ``DIGITS`` means, not a different algorithm.
"""

from __future__ import annotations

import base64
import time

import pytest

from cara.authentication import TwoFactor

BACKUP_CODE_COUNT = TwoFactor.BACKUP_CODE_COUNT
DIGITS = TwoFactor.DIGITS
STEP_SECONDS = TwoFactor.STEP_SECONDS
_hotp = TwoFactor._hotp
generate_backup_codes = TwoFactor.generate_backup_codes
generate_secret = TwoFactor.generate_secret
matched_totp_counter = TwoFactor.matched_totp_counter
normalize_backup_code = TwoFactor.normalize_backup_code
provisioning_uri = TwoFactor.provisioning_uri
verify_totp = TwoFactor.verify_totp

# RFC 4226 / RFC 6238 test seed: the ASCII string "12345678901234567890".
_RFC_SECRET = base64.b32encode(b"12345678901234567890").decode("ascii").rstrip("=")


class TestRfc4226Vectors:
    """Appendix D — HOTP values for counters 0..9 on the reference seed."""

    @pytest.mark.parametrize(
        "counter,expected",
        [
            (0, "755224"),
            (1, "287082"),
            (2, "359152"),
            (3, "969429"),
            (4, "338314"),
            (5, "254676"),
            (6, "287922"),
            (7, "162583"),
            (8, "399871"),
            (9, "520489"),
        ],
    )
    def test_hotp_matches_the_published_value(self, counter: int, expected: str):
        assert _hotp(_RFC_SECRET, counter) == expected


class TestRfc6238Vectors:
    """Appendix B — SHA-1 TOTP at the published instants."""

    @pytest.mark.parametrize(
        "at,expected",
        [
            (59, "287082"),
            (1111111109, "081804"),
            (1111111111, "050471"),
            (1234567890, "005924"),
            (2000000000, "279037"),
            (20000000000, "353130"),
        ],
    )
    def test_totp_matches_the_published_value(self, at: int, expected: str):
        # window=0 so the assertion is about THIS step, not a neighbour.
        assert verify_totp(_RFC_SECRET, expected, window=0, at=at) is True

    def test_the_expected_step_counter_is_returned(self):
        assert matched_totp_counter(_RFC_SECRET, "287082", window=0, at=59) == 1

    def test_a_neighbouring_step_code_fails_without_a_window(self):
        # 287082 is the counter-1 code; at t=0 (counter 0) it is not current.
        assert verify_totp(_RFC_SECRET, "287082", window=0, at=0) is False


class TestClockSkewWindow:
    def test_the_previous_step_is_accepted_within_the_window(self):
        # counter 0 code, checked one step later.
        assert matched_totp_counter(_RFC_SECRET, "755224", window=1, at=STEP_SECONDS) == 0

    def test_the_next_step_is_accepted_within_the_window(self):
        # counter 2 code, checked one step early (at t=30s, counter 1).
        assert matched_totp_counter(_RFC_SECRET, "359152", window=1, at=STEP_SECONDS) == 2

    def test_a_window_reaching_below_counter_zero_still_checks_the_rest(self):
        """The counter packs as an UNSIGNED 64-bit int, so a negative
        candidate raises. It must be skipped, not allowed to abort the whole
        window — otherwise a verification fails on the clock, not the code."""
        assert matched_totp_counter(_RFC_SECRET, "287082", window=1, at=0) == 1
        assert matched_totp_counter(_RFC_SECRET, "755224", window=5, at=0) == 0

    def test_two_steps_away_is_outside_a_one_step_window(self):
        assert (
            matched_totp_counter(_RFC_SECRET, "755224", window=1, at=2 * STEP_SECONDS)
            is None
        )

    def test_a_wider_window_reaches_further(self):
        assert (
            matched_totp_counter(_RFC_SECRET, "755224", window=2, at=2 * STEP_SECONDS)
            == 0
        )

    def test_the_counter_identifies_the_step_a_caller_must_burn(self):
        """Replay defence lives in the caller, and this is the handle it needs.

        Accepting a ±1-step window means one code stays valid across three
        steps; without the counter the caller has nothing to mark spent.
        """
        first = matched_totp_counter(_RFC_SECRET, "287082", window=1, at=59)
        later = matched_totp_counter(
            _RFC_SECRET, "287082", window=1, at=59 + STEP_SECONDS
        )
        assert first == later == 1


class TestMalformedInput:
    @pytest.mark.parametrize(
        "code", ["", "12345", "1234567", "abcdef", "12 34 5", None, "  "]
    )
    def test_a_code_that_is_not_six_digits_is_refused(self, code):
        assert matched_totp_counter(_RFC_SECRET, code, at=59) is None

    @pytest.mark.parametrize("secret", ["", None])
    def test_a_missing_secret_is_refused(self, secret):
        assert matched_totp_counter(secret, "287082", at=59) is None

    def test_a_secret_that_is_not_base32_is_refused_not_raised(self):
        assert matched_totp_counter("!!!not-base32!!!", "287082", at=59) is None

    def test_surrounding_whitespace_and_spacing_are_tolerated(self):
        """Authenticator apps render "287 082"; users paste it that way."""
        assert verify_totp(_RFC_SECRET, "  287 082 ", window=0, at=59) is True


class TestSecretGeneration:
    def test_a_generated_secret_is_unpadded_base32_that_still_decodes(self):
        secret = generate_secret()
        assert "=" not in secret
        assert base64.b32decode(secret + "=" * (-len(secret) % 8)) is not None

    def test_a_generated_secret_carries_160_bits(self):
        secret = generate_secret()
        assert len(base64.b32decode(secret + "=" * (-len(secret) % 8))) == 20

    def test_secrets_do_not_repeat(self):
        assert len({generate_secret() for _ in range(50)}) == 50

    def test_a_generated_secret_round_trips_through_verification(self):
        secret = generate_secret()
        now = time.time()
        code = _hotp(secret, int(now // STEP_SECONDS))
        assert verify_totp(secret, code, window=0, at=now) is True


class TestProvisioningUri:
    def test_the_uri_carries_the_parameters_this_implementation_actually_uses(self):
        uri = provisioning_uri("ABCDEF", account="a@example.com", issuer="Acme")
        assert uri.startswith("otpauth://totp/")
        assert "secret=ABCDEF" in uri
        assert "issuer=Acme" in uri
        assert f"digits={DIGITS}" in uri
        assert f"period={STEP_SECONDS}" in uri
        assert "algorithm=SHA1" in uri

    def test_the_label_is_escaped_so_a_colon_cannot_split_it(self):
        """An account or issuer containing ':' or '/' would otherwise forge a
        different label — the part every authenticator app displays."""
        uri = provisioning_uri("ABCDEF", account="a@example.com", issuer="Ac:me/Corp Ltd")
        label = uri[len("otpauth://totp/") : uri.index("?")]
        assert ":" not in label
        assert "/" not in label
        assert " " not in label


class TestBackupCodes:
    def test_the_default_count_is_generated(self):
        assert len(generate_backup_codes()) == BACKUP_CODE_COUNT

    def test_an_explicit_count_is_honoured(self):
        assert len(generate_backup_codes(3)) == 3

    def test_codes_are_grouped_and_unambiguous(self):
        for code in generate_backup_codes():
            assert len(code) == 11
            assert code[5] == "-"
            # 0/O and 1/I/L are excluded so a hand-typed code cannot fail on
            # a look-alike glyph.
            assert not set(code[:5] + code[6:]) & set("01oOiIlL")

    def test_codes_do_not_repeat(self):
        assert len(set(generate_backup_codes(50))) == 50

    @pytest.mark.parametrize(
        "typed", ["ab2cd-3ef4g", "AB2CD-3EF4G", "  ab2cd 3ef4g  ", "ab2cd3ef4g"]
    )
    def test_normalization_is_case_and_separator_insensitive(self, typed: str):
        assert normalize_backup_code(typed) == "ab2cd3ef4g"

    def test_normalizing_nothing_yields_an_empty_string_not_an_error(self):
        assert normalize_backup_code(None) == ""
        assert normalize_backup_code("") == ""

    def test_a_generated_code_normalizes_to_its_own_alphabet(self):
        for code in generate_backup_codes(5):
            assert normalize_backup_code(code).isalnum()
            assert len(normalize_backup_code(code)) == 10
