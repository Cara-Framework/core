"""Sign-in hints — who was here, carried outside the browser's storage.

An account picker that only remembers in ``localStorage`` forgets everyone
the moment the visitor clears site data, switches device, or opens a private
window — and a blank email field is exactly the friction the picker existed
to remove. Nothing origin-scoped survives that clear, so the recall has to
live somewhere the origin does not own: the URL of a link the visitor still
has (the sign-in email in their inbox, a bookmark, a session-expiry bounce).

A hint is therefore a short, self-expiring, URL-safe ciphertext carrying a
few non-secret labels — an address, a display name, whichever workspace the
link belongs to. It is **encrypted, not signed**: a signed hint would leave
a plaintext email address sitting in browser history, ``Referer`` headers,
proxy logs, and anything that indexes URLs. Only the issuing application can
read one back.

A hint is emphatically NOT a credential. It selects which account the sign-in
form is about; the sign-in itself still has to happen. Treat a forged or
stale hint as a cosmetic miss — ``read`` answers ``None`` and the caller
falls back to an empty form.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

import pendulum

from cara.facades import Crypt, Log

#: Long enough that a link found in an old email still recognises its owner,
#: short enough that an abandoned machine stops volunteering an address.
DEFAULT_TTL_DAYS = 30

#: A hint rides in a query string alongside a return path and sometimes a
#: single-use token; past this it stops being a URL that survives mail
#: clients and starts being one they wrap or truncate.
MAX_TOKEN_LENGTH = 512


class SignInHint:
    """Mint and read the non-secret identity hint an auth link carries."""

    DEFAULT_TTL_DAYS = DEFAULT_TTL_DAYS
    MAX_TOKEN_LENGTH = MAX_TOKEN_LENGTH

    @classmethod
    def mint(
        cls,
        claims: Mapping[str, Any],
        *,
        ttl_days: int = DEFAULT_TTL_DAYS,
    ) -> str | None:
        """Encrypt ``claims`` into a URL-safe hint, or ``None``.

        ``None`` covers every reason a hint cannot be produced — empty
        claims, an unconfigured keyring, an oversized payload. Callers append
        the hint when there is one and emit the link either way; a missing
        hint costs a visitor one typed email address, while a raised
        exception would cost them the entire email.
        """
        payload = {key: value for key, value in claims.items() if value not in (None, "")}
        if not payload:
            return None
        try:
            envelope = json.dumps(
                {"c": payload, "x": pendulum.now("UTC").add(days=ttl_days).int_timestamp},
                separators=(",", ":"),
                sort_keys=True,
            )
            token = Crypt.encrypt_urlsafe(envelope)
            if len(token) > MAX_TOKEN_LENGTH:
                # Past the budget it stops being a URL that mail clients
                # forward intact, so an oversized hint is worse than none.
                cls._note(f"sign-in hint discarded: {len(token)} bytes over budget")
                return None
            return token
        except Exception as exc:
            cls._note(f"sign-in hint could not be minted: {exc}")
            return None

    @staticmethod
    def _note(message: str) -> None:
        """Log without ever becoming the reason a caller fails.

        Minting sits inside a logout response and an outbound email — work
        that must complete whether or not a hint does. Logging is the last
        thing here that can still throw (an unbootstrapped or misconfigured
        logger facade), so it is the last thing that gets swallowed.
        """
        with suppress(Exception):
            Log.warning(message, category="auth")

    @classmethod
    def read(cls, token: object) -> dict[str, Any] | None:
        """Decrypt a hint back to its claims, or ``None`` if unusable.

        Unusable covers forged, truncated, expired, and minted-under-a-
        retired-key. None of those are errors worth surfacing: the visitor
        simply gets the plain form they would have had without a hint.
        """
        if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_LENGTH:
            return None
        try:
            envelope = json.loads(Crypt.decrypt_urlsafe(token))
        except Exception:
            # Silent by design. This decrypts attacker-supplied input on a
            # public endpoint, so a failure is the expected case and logging
            # each one hands anyone a free way to flood the log.
            return None
        if not isinstance(envelope, dict):
            return None
        expires_at = envelope.get("x")
        if (
            not isinstance(expires_at, int)
            or expires_at <= pendulum.now("UTC").int_timestamp
        ):
            return None
        claims = envelope.get("c")
        return claims if isinstance(claims, dict) else None
