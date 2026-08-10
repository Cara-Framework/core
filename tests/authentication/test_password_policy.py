"""Password policy — thresholds, config overrides, and the truncation clamp.

The interesting behaviour here is the byte ceiling, which answers two
questions at once: a configurable cost bound, and a non-negotiable clamp to
whatever the storage algorithm can actually authenticate. Products had
drifted to 72 and 1024 respectively by conflating the two; these tests pin
that both survive and that configuration can lower the ceiling but never
raise it past the algorithm's own limit.
"""

from __future__ import annotations

import pytest

from cara.authentication.PasswordPolicy import (
    COMMON_PASSWORD_PREFIXES,
    MAX_PASSWORD_BYTES,
    MIN_UNIQUE_CHARS,
    check_password_strength,
    denied_prefixes,
    password_max_bytes,
)
from cara.configuration import Configuration
from cara.exceptions import InvalidArgumentException


@pytest.fixture
def policy_config(monkeypatch: pytest.MonkeyPatch):
    """Set ``auth.*`` policy keys on the Configuration singleton.

    ``monkeypatch.setitem`` restores the singleton's dict after each test, so
    nothing leaks between cases.
    """
    Configuration()  # ensure the bare singleton exists
    store = Configuration._instance._config

    def _set(**values: object) -> None:
        for name, value in values.items():
            monkeypatch.setitem(store, f"auth.{name}", value)

    # Start from a known-clean slate: another config file loaded earlier in
    # the session must not decide this module's defaults.
    for key in (
        "password_max_bytes",
        "password_min_unique_chars",
        "password_denied_prefixes",
        "password_hash_algorithm",
    ):
        monkeypatch.setitem(store, f"auth.{key}", None)
    return _set


class TestDefaults:
    def test_a_reasonable_password_passes(self, policy_config):
        assert check_password_strength("MyS3cure!Pass") is None

    def test_the_default_ceiling_is_the_cost_bound(self, policy_config):
        assert password_max_bytes() == MAX_PASSWORD_BYTES == 1024

    def test_a_password_at_the_ceiling_passes(self, policy_config):
        assert check_password_strength("aBcD" * 256) is None  # exactly 1024 bytes

    def test_a_password_past_the_ceiling_is_refused(self, policy_config):
        error = check_password_strength("aBcD" * 256 + "x")
        assert error == (
            "Password must be at most 1024 bytes (the encoded value is too large)."
        )

    def test_the_ceiling_counts_BYTES_not_characters(self, policy_config):
        """A 4-byte-per-character password must not slip past a byte bound
        by being short in characters."""
        four_byte_chars = "🔒🔑🎁🎈" * 64  # 256 characters, 1024 bytes
        assert len(four_byte_chars.encode("utf-8")) == MAX_PASSWORD_BYTES
        assert check_password_strength(four_byte_chars) is None
        assert "bytes" in (check_password_strength(four_byte_chars + "🔒") or "")


class TestUniqueCharacters:
    @pytest.mark.parametrize("password", ["aaaaaaaa", "abababab", "abcabcabc"])
    def test_too_few_distinct_characters_is_refused(self, password, policy_config):
        assert check_password_strength(password) == (
            "Password must contain at least 4 different characters."
        )

    def test_the_default_floor_is_four(self, policy_config):
        assert MIN_UNIQUE_CHARS == 4
        assert check_password_strength("abcd") is None

    def test_the_floor_is_configurable(self, policy_config):
        policy_config(password_min_unique_chars=6)
        assert check_password_strength("abcde") == (
            "Password must contain at least 6 different characters."
        )
        assert check_password_strength("abcdef") is None

    @pytest.mark.parametrize("bad", ["not-a-number", 0, -3, True, ""])
    def test_an_unusable_configured_floor_falls_back_to_the_default(
        self, bad, policy_config
    ):
        """A malformed threshold must not silently switch the bound off."""
        policy_config(password_min_unique_chars=bad)
        assert check_password_strength("aaaa") is not None
        assert check_password_strength("abcd") is None


class TestCommonPrefixes:
    @pytest.mark.parametrize("prefix", COMMON_PASSWORD_PREFIXES)
    def test_every_framework_default_prefix_is_refused(self, prefix, policy_config):
        assert check_password_strength(prefix + "9xZ!") == (
            "This password is too common. Please choose a different password."
        )

    def test_matching_is_case_insensitive(self, policy_config):
        assert check_password_strength("PASSWORD123X") is not None

    def test_a_product_can_extend_the_deny_list(self, policy_config):
        policy_config(password_denied_prefixes=["hunter2", " ACME "])
        assert "hunter2" in denied_prefixes()
        assert check_password_strength("hunter2Xy!") is not None
        assert check_password_strength("acmeRocks!42") is not None

    def test_a_product_cannot_shrink_the_deny_list(self, policy_config):
        policy_config(password_denied_prefixes=["hunter2"])
        assert set(COMMON_PASSWORD_PREFIXES) <= set(denied_prefixes())

    def test_a_single_string_is_accepted_as_a_one_entry_list(self, policy_config):
        policy_config(password_denied_prefixes="hunter2")
        assert check_password_strength("hunter2Xy!") is not None


class TestTruncationClamp:
    """bcrypt reads 72 bytes and no more. A policy over bcrypt storage that
    accepted longer input would mint accounts that can be created and then
    never signed into, because ``BcryptHasher.check`` refuses over-long
    input outright."""

    def test_bcrypt_storage_clamps_the_ceiling_to_72(self, policy_config):
        policy_config(password_hash_algorithm="bcrypt")
        assert password_max_bytes() == 72
        assert check_password_strength("a1B!" * 18) is None  # exactly 72 bytes
        assert check_password_strength("a1B!" * 18 + "x") == (
            "Password must be at most 72 bytes (the encoded value is too large)."
        )

    def test_configuration_cannot_raise_past_the_truncation_bound(self, policy_config):
        policy_config(password_hash_algorithm="bcrypt", password_max_bytes=4096)
        assert password_max_bytes() == 72

    def test_configuration_can_still_lower_the_ceiling(self, policy_config):
        policy_config(password_hash_algorithm="bcrypt", password_max_bytes=32)
        assert password_max_bytes() == 32

    def test_argon2id_storage_keeps_the_full_cost_bound(self, policy_config):
        policy_config(password_hash_algorithm="argon2id")
        assert password_max_bytes() == MAX_PASSWORD_BYTES

    def test_the_cost_bound_is_configurable_on_a_non_truncating_algorithm(
        self, policy_config
    ):
        policy_config(password_max_bytes=256)
        assert password_max_bytes() == 256
        assert check_password_strength("aBcD" * 65) is not None

    @pytest.mark.parametrize("bad", ["nope", 0, -1, True])
    def test_an_unusable_configured_ceiling_falls_back_to_the_default(
        self, bad, policy_config
    ):
        policy_config(password_max_bytes=bad)
        assert password_max_bytes() == MAX_PASSWORD_BYTES

    def test_an_unknown_storage_algorithm_raises_rather_than_guessing(
        self, policy_config
    ):
        """Falling back would silently restore the full cost bound over
        storage whose limits we do not know — the exact failure this clamp
        exists to prevent."""
        policy_config(password_hash_algorithm="rot13")
        with pytest.raises(InvalidArgumentException):
            password_max_bytes()


class TestOrderOfChecks:
    def test_the_byte_bound_is_evaluated_before_anything_expensive(self, policy_config):
        """A megabyte of 'a' is both oversized and low-entropy; the ceiling
        message is the one that must come back, because bounding the input
        is the whole reason the check runs first."""
        assert "bytes" in (check_password_strength("a" * 2_000_000) or "")

    def test_an_empty_password_is_refused_on_distinct_characters(self, policy_config):
        assert check_password_strength("") == (
            "Password must contain at least 4 different characters."
        )
