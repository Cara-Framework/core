"""The unsubscribe wire format is frozen, and this is what freezes it.

Mint and verify live in different processes and the token sits in a mailbox
in between, so nothing in the system reconciles the two sides. A drift of
one byte silently kills every unsubscribe link already sent — which has
happened before. The fixed vector below is the contract: if it changes,
links in real mailboxes have stopped working.
"""

from __future__ import annotations

import hashlib
import hmac
import sys

import pytest

from cara.notifications.UnsubscribeToken import matches, mint

_PUBLIC_ID = "usr_9f2c1a"
_EMAIL = "reader@example.com"
_SECRET = "s3cret-unsubscribe-key"

# Computed independently of the implementation, from the documented format:
# hexdigest of HMAC-SHA256 over "<public_id>:<email>" keyed by the raw secret.
_EXPECTED = hmac.new(
    _SECRET.encode("utf-8"),
    f"{_PUBLIC_ID}:{_EMAIL}".encode(),
    hashlib.sha256,
).hexdigest()


def test_the_wire_format_is_the_documented_one() -> None:
    assert mint(_PUBLIC_ID, _EMAIL, _SECRET) == _EXPECTED
    # Hex, not base64 or raw digest bytes — a "tidier" encoding is exactly
    # the kind of improvement that breaks every link already mailed.
    assert len(_EXPECTED) == 64
    assert set(_EXPECTED) <= set("0123456789abcdef")


def test_the_token_verifies_against_the_recipient_it_was_minted_for() -> None:
    assert matches(mint(_PUBLIC_ID, _EMAIL, _SECRET), _PUBLIC_ID, _EMAIL, _SECRET)


@pytest.mark.parametrize(
    ("public_id", "email", "secret"),
    [
        ("usr_other", _EMAIL, _SECRET),  # someone else's link
        (_PUBLIC_ID, "other@example.com", _SECRET),  # forwarded to another address
        (_PUBLIC_ID, _EMAIL, "different-secret"),  # rotated / wrong secret
    ],
)
def test_a_token_does_not_verify_for_a_different_recipient_or_secret(
    public_id: str, email: str, secret: str
) -> None:
    assert not matches(mint(_PUBLIC_ID, _EMAIL, _SECRET), public_id, email, secret)


def test_a_tampered_token_is_rejected() -> None:
    token = mint(_PUBLIC_ID, _EMAIL, _SECRET)
    flipped = ("0" if token[0] != "0" else "1") + token[1:]

    assert not matches(flipped, _PUBLIC_ID, _EMAIL, _SECRET)


@pytest.mark.parametrize("blank", ["", None])
def test_minting_without_a_secret_raises_instead_of_signing_nothing(blank) -> None:
    """A token minted from a blank secret is forgeable, not merely weak.

    The caller must decide whether to send the mail with no link at all;
    the framework must not hand back something that looks like a signature.
    """
    with pytest.raises(ValueError):
        mint(_PUBLIC_ID, _EMAIL, blank)
    with pytest.raises(ValueError):
        mint(blank, _EMAIL, _SECRET)
    with pytest.raises(ValueError):
        mint(_PUBLIC_ID, blank, _SECRET)


def test_verification_is_constant_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Comparison must route through ``hmac.compare_digest``, not ``==``.

    A byte-by-byte ``==`` on a signature leaks how far the attacker's
    guess matched, which is enough to walk a valid token out one
    character at a time. This pin used to live in a product's own test of
    its own hand-rolled verifier; the comparison lives here now, so the
    law does too.
    """
    # Patch through sys.modules, not through the package attribute: a
    # package attribute can be shadowed by a barrel export, and then the
    # patch lands somewhere the code under test never looks.
    module = sys.modules["cara.notifications.UnsubscribeToken"]

    calls: list[tuple] = []
    real = hmac.compare_digest

    def spy(a, b):
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(module.hmac, "compare_digest", spy)

    assert matches(mint(_PUBLIC_ID, _EMAIL, _SECRET), _PUBLIC_ID, _EMAIL, _SECRET)
    assert calls, "verifier did not route through hmac.compare_digest"


@pytest.mark.parametrize("blank", ["", None])
def test_verifying_with_anything_missing_is_a_plain_no(blank) -> None:
    """The verifier's answer is "no" for a misconfiguration too.

    Raising here would tempt a caller into treating a missing secret as a
    distinct, more permissive outcome than a bad token.
    """
    token = mint(_PUBLIC_ID, _EMAIL, _SECRET)

    assert not matches(blank, _PUBLIC_ID, _EMAIL, _SECRET)
    assert not matches(token, _PUBLIC_ID, _EMAIL, blank)
    assert not matches(token, blank, _EMAIL, _SECRET)
    assert not matches(token, _PUBLIC_ID, blank, _SECRET)
