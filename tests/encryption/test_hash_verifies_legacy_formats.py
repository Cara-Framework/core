"""Hash.check follows the stored artifact's own algorithm.

The day DEFAULT_ALGORITHM moved (bcrypt → argon2id), every check of a
legacy bcrypt hash routed to the argon driver, verification returned
False for the right password, and every pre-migration user was silently
locked out. Verification must dispatch on the hash's own prefix; the
default only governs ``make``.
"""

from __future__ import annotations

from cara.encryption import Hash


def test_legacy_bcrypt_hash_still_verifies_under_the_argon_default():
    password = "correct horse battery staple"
    stored = Hash.make(password, algorithm="bcrypt")
    assert stored.startswith("$2")

    assert Hash.check(password, stored) is True
    assert Hash.check("wrong password", stored) is False


def test_default_algorithm_hashes_verify_without_naming_it():
    password = "correct horse battery staple"
    stored = Hash.make(password)
    assert Hash.check(password, stored) is True


def test_explicit_algorithm_still_wins():
    password = "pw"
    stored = Hash.make(password, algorithm="bcrypt")
    # Forcing the wrong driver is the caller's decision — honored as told.
    assert Hash.check(password, stored, algorithm="argon2id") is False


def test_legacy_hash_reports_needing_a_rehash_under_the_new_default():
    stored = Hash.make("pw", algorithm="bcrypt")
    assert Hash.needs_rehash(stored) is True
