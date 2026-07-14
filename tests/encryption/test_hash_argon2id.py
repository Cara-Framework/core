from __future__ import annotations

from cara.encryption import Hash


def test_argon2id_is_the_default_and_verifies() -> None:
    hashed = Hash.make("a long correct horse battery staple")

    assert hashed.startswith("$argon2id$")
    assert Hash.check("a long correct horse battery staple", hashed)
    assert not Hash.check("wrong", hashed)
    assert not Hash.needs_rehash(hashed)


def test_legacy_bcrypt_is_verified_then_marked_for_upgrade() -> None:
    hashed = Hash.make("legacy password", algorithm="bcrypt", rounds=4)

    assert Hash.check("legacy password", hashed)
    assert Hash.needs_rehash(hashed)
    assert not Hash.check("x" * 73, hashed)
