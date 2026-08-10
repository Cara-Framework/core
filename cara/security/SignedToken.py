"""Purpose-scoped signed claims — a compact token you can hand to a stranger.

Some flows leave the application's auth context entirely and have to come
back: an OAuth consent redirect, a one-click link in an email, a callback a
third party invokes unauthenticated. The only thread back is whatever the
request carries, so that value must be self-describing, tamper-evident and
short-lived. This class is that value.

``issue`` produces ``<base64url(json claims envelope)>.<hex hmac-sha256>``.
``verify`` returns the claims only when every check passes and ``None``
otherwise — never a partially-trusted result, never an exception a caller
might mistake for a transport failure.

PURPOSE IS PART OF THE KEY, NOT PART OF THE PAYLOAD
---------------------------------------------------
Each caller signs under a key derived from ``APP_KEY`` and its own purpose
label, so a token minted for one flow produces a signature mismatch in
another. Domain separation belongs in the key rather than in a payload field
a verifier can forget to compare: forgetting to derive the key is a hard
failure at the first round-trip, while forgetting to compare a field is
silent. Without this, an email token and an OAuth state token signed by the
same ``APP_KEY`` would be interchangeable.

VERIFICATION IS FAIL-CLOSED, IN THIS ORDER
-------------------------------------------
Shape (type, length, exactly one separator, hex signature of the right
width) is checked before any decoding, so malformed input costs a couple of
comparisons rather than a JSON parse. Then the MAC, in constant time. Only
then is the body decoded — with a canonical re-encode check, so a token
cannot be re-spelled into a different string naming the same claims.
Finally the envelope: exact key set, version, an integer (never ``bool``)
expiry that is both un-expired and not further away than the caller's own
maximum age. That last upper bound is what stops a token minted under a
mistaken TTL from outliving the flow it belongs to.

Replay is deliberately NOT handled here: a signed claim is stateless by
construction. A caller that must accept a token once binds it to state it
owns — a stored nonce hash, a spent-counter marker — exactly as
``cara.authentication.TwoFactor`` exposes a counter for its caller to burn.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from typing import Any

from cara.configuration import config


class SignedToken:
    """Mint and verify purpose-scoped, TTL-bound signed claims."""

    # A signed token rides in a URL or a header. Anything past this is not a
    # token we issued, and bounding it before decoding keeps a huge body
    # from reaching the JSON parser.
    MAX_TOKEN_LENGTH = 2048

    # The signing key must be able to carry a 256-bit MAC. Anything shorter
    # is a misconfiguration, not a weaker mode: refuse rather than sign.
    _MIN_KEY_BYTES = 32
    _SIGNATURE_HEX_LENGTH = 64  # sha256, hex-encoded
    _VERSION = 1
    _ENVELOPE_KEYS = {"v", "e", "c"}
    # Key-derivation domain. Bumping this label invalidates every
    # outstanding token, which is the intended emergency lever.
    _KEY_LABEL = b"cara.signed-token.v1."

    @classmethod
    def issue(cls, claims: Mapping[str, Any], *, purpose: str, ttl: int) -> dict:
        """Mint a signed token carrying ``claims``, valid for ``ttl`` seconds.

        Returns ``{"token": str, "expires_at": int}`` — the caller usually
        needs to persist the expiry beside whatever state it binds the token
        to.
        """
        if not isinstance(claims, Mapping):
            raise TypeError("Signed-token claims must be a mapping.")
        if ttl <= 0:
            raise ValueError("Signed-token ttl must be a positive number of seconds.")
        payload = dict(claims)
        if any(not isinstance(name, str) for name in payload):
            raise TypeError("Signed-token claim names must be strings.")

        expires_at = int(time.time()) + int(ttl)
        envelope = json.dumps(
            {"v": cls._VERSION, "e": expires_at, "c": payload},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        body = base64.urlsafe_b64encode(envelope).rstrip(b"=").decode()
        signature = hmac.new(cls._key(purpose), body.encode(), hashlib.sha256).hexdigest()
        return {"token": f"{body}.{signature}", "expires_at": expires_at}

    @classmethod
    def verify(cls, token: str | None, *, purpose: str, max_ttl: int) -> dict | None:
        """Return ``{"claims": dict, "expires_at": int}``, or ``None`` on any fault.

        ``max_ttl`` must be the same lifetime the issuing side used (or
        longer); a token whose expiry sits further into the future than that
        is rejected rather than honoured.
        """
        if (
            not isinstance(token, str)
            or not token
            or len(token) > cls.MAX_TOKEN_LENGTH
            or token.count(".") != 1
        ):
            return None
        body, signature = token.split(".", 1)
        if not body or len(signature) != cls._SIGNATURE_HEX_LENGTH:
            return None
        try:
            bytes.fromhex(signature)
            body_bytes = body.encode("ascii")
        except ValueError, UnicodeEncodeError:
            return None

        expected = hmac.new(cls._key(purpose), body_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None

        try:
            padded = body_bytes + b"=" * (-len(body_bytes) % 4)
            decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
            # ``validate=True`` polices the ALPHABET, not canonical spelling:
            # the unused low bits of the final character decode to the same
            # payload whatever they hold. Re-encoding and comparing is what
            # makes the token string a unique NAME for its claims — without
            # it a caller tracking single use by token identity would see two
            # names for one token.
            if base64.urlsafe_b64encode(decoded).rstrip(b"=") != body_bytes:
                return None
            envelope = json.loads(decoded)
        except ValueError, TypeError, UnicodeDecodeError:
            return None

        if not isinstance(envelope, dict) or set(envelope) != cls._ENVELOPE_KEYS:
            return None
        if envelope.get("v") != cls._VERSION:
            return None

        expires_at = envelope.get("e")
        now = time.time()
        if (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, int)
            or expires_at < now
            or expires_at > now + max_ttl
        ):
            return None

        claims = envelope.get("c")
        if not isinstance(claims, dict) or any(
            not isinstance(name, str) for name in claims
        ):
            return None
        return {"claims": claims, "expires_at": expires_at}

    @classmethod
    def _key(cls, purpose: str) -> bytes:
        """Derive this purpose's signing key from ``APP_KEY``.

        Read at call time: configuration is loaded during boot, and a key
        captured at import would pin whatever (possibly nothing) was set
        then.
        """
        if not isinstance(purpose, str) or not purpose.strip():
            raise ValueError("A signed token needs a non-empty purpose label.")
        root = str(config("app.key", "") or "").encode()
        if len(root) < cls._MIN_KEY_BYTES:
            raise RuntimeError(
                f"APP_KEY must contain at least {cls._MIN_KEY_BYTES} bytes "
                "to sign tokens."
            )
        return hmac.new(
            root, cls._KEY_LABEL + purpose.encode("utf-8"), hashlib.sha256
        ).digest()


__all__ = ["SignedToken"]
