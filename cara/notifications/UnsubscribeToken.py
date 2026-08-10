"""The unsubscribe link's signature — minted and verified in one place.

An unsubscribe link is minted by whatever process sends the mail and
verified by whatever process serves the click. Those are different
deployables, often different repositories, and the token is just bytes in
a mailbox in between. Nothing reconciles them: if the two sides drift by a
single byte, every legitimate unsubscribe link in every mailbox already
sent stops working, silently, and the only symptom is a reader who cannot
opt out — which is a legal problem, not a cosmetic one.

That drift has happened: one issuer once derived a plain SHA-256 of
``"<id>:<email>:unsub"`` with no secret at all, so links were both
forgeable and universally rejected by the verifier, and the unsubscribe
call-to-action was dead for as long as nobody clicked it in anger.

So the wire format lives here, once, and both halves call it.

FORMAT — frozen. ``hexdigest`` of HMAC-SHA256 over ``"<public_id>:<email>"``
keyed by the raw secret. It is pinned against a fixed vector in
``tests/notifications/test_unsubscribe_token.py``. Changing any part of it
invalidates every link already in a mailbox, so it may only change behind a
new parameter that lets a verifier accept both during a migration — never
by editing this function.

Sign the OPAQUE public id, never an internal row id: this value lands in a
mailbox, gets forwarded, and sits in mail logs, so it must not expose the
id space or let anyone probe it by decrementing a number.
"""

from __future__ import annotations

import hashlib
import hmac

__all__ = ["matches", "mint"]


def mint(public_id: str, email: str, secret: str) -> str:
    """The unsubscribe token for one recipient.

    Args:
        public_id: The recipient's opaque public identifier.
        email: The address the link is being mailed to.
        secret: The application's unsubscribe signing secret.

    Returns:
        The hex signature to put in the link's ``token`` parameter.

    Raises:
        ValueError: If any part is empty. A token minted from a blank
            secret is not a weak token, it is a forgeable one, and the
            caller must decide whether to send the mail without a link
            rather than silently ship an unsigned one.
    """
    if not public_id or not email or not secret:
        raise ValueError(
            "unsubscribe token needs a public_id, an email and a non-empty secret"
        )
    return hmac.new(
        secret.encode("utf-8"),
        f"{public_id}:{email}".encode(),
        hashlib.sha256,
    ).hexdigest()


def matches(token: str, public_id: str, email: str, secret: str) -> bool:
    """Whether ``token`` is the signature this recipient's link should carry.

    Compared in constant time. Returns ``False`` rather than raising for
    anything unusable — a missing secret, a blank token — because the
    verifier's answer to "is this link valid" is no in every one of those
    cases, and an exception would only tempt a caller into treating a
    configuration fault as a distinct, more permissive outcome.
    """
    if not token or not public_id or not email or not secret:
        return False
    return hmac.compare_digest(token, mint(public_id, email, secret))
