from __future__ import annotations

from cara.encryption import Hash


def test_argon2id_is_the_default_and_verifies() -> None:
    hashed = Hash.make("a long correct horse battery staple")

    assert hashed.startswith("$argon2id$")
    assert Hash.check("a long correct horse battery staple", hashed)
    assert not Hash.check("wrong", hashed)
    assert not Hash.needs_rehash(hashed)


def test_hash_verification_never_guesses_the_storage_algorithm() -> None:
    hashed = Hash.make("bcrypt password", algorithm="bcrypt", rounds=4)

    assert not Hash.check("bcrypt password", hashed)
    assert Hash.check("bcrypt password", hashed, algorithm="bcrypt")
    assert Hash.needs_rehash(hashed, algorithm="bcrypt", rounds=12)
    assert not Hash.needs_rehash(hashed, algorithm="bcrypt", rounds=4)
    assert not Hash.check("x" * 73, hashed, algorithm="bcrypt")
