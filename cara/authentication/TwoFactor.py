"""Two-factor primitives — RFC 6238 TOTP + single-use backup codes.

Hand-rolled (no ``pyotp`` dependency): a TOTP is HMAC-SHA1 over the
30-second time counter, truncated to 6 digits (RFC 6238 / RFC 4226). The
shared secret is a base32 string the authenticator app imports through the
``otpauth://`` provisioning URI (rendered as a QR by the caller).
Verification allows a ±1 step window so a slightly-skewed device clock
still passes.

Handling rules for callers, enforced by convention rather than by this
class — it holds no state, so it cannot enforce them itself:

- the secret is sensitive — persist it ENCRYPTED (``Crypt``), never log it,
  and only ever hand the plaintext secret to the enrolling user once;
- backup codes are single-use — store only their hash, show the plaintext
  list exactly once at generation, and burn a code on use;
- an accepted TOTP must be marked spent by its counter (see
  ``matched_totp_counter``), because the acceptance window alone would leave
  every accepted code replayable for the width of that window.

Pure functions only; no DB, no framework state — so an enrollment route, a
login step-up and any future re-authentication path all resolve TOTP
identically.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
from urllib.parse import quote


class TwoFactor:
    """Stateless TOTP and backup-code primitives."""

    STEP_SECONDS = 30
    DIGITS = 6
    BACKUP_CODE_COUNT = 10

    _SECRET_BYTES = 20  # 160-bit shared secret — the RFC 4226 recommendation.
    # No ambiguous glyphs (0/O, 1/I/L) so a hand-typed backup code can't fail
    # on a look-alike. 30-symbol alphabet × 10 chars ≈ 49 bits per code.
    _BACKUP_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz"

    @classmethod
    def generate_secret(cls) -> str:
        """A fresh base32 TOTP secret, unpadded (what authenticator apps expect)."""
        return (
            base64.b32encode(secrets.token_bytes(cls._SECRET_BYTES))
            .decode("ascii")
            .rstrip("=")
        )

    @staticmethod
    def _pad_b32(secret: str) -> str:
        """base32 decode needs 8-char-multiple length; app-facing secrets drop padding."""
        return secret + ("=" * ((-len(secret)) % 8))

    @classmethod
    def _hotp(cls, secret: str, counter: int) -> str:
        key = base64.b32decode(cls._pad_b32(secret), casefold=True)
        digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
        offset = digest[-1] & 0x0F
        truncated = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
        return str(truncated % (10**cls.DIGITS)).zfill(cls.DIGITS)

    @classmethod
    def current_code(cls, secret: str, *, at: float | None = None) -> str:
        """The TOTP ``secret`` produces right now — the generating half.

        Only verification was public, so anything needing to PRODUCE a code
        (a development enrollment command, an end-to-end login that must get
        past the 2FA wall) had to reach into ``_hotp`` and re-derive the time
        step by hand. Two derivations of one formula is one too many; this is
        the same arithmetic ``matched_totp_counter`` checks against.

        ``at`` (unix seconds) is injectable for tests; otherwise the wall
        clock, exactly as verification reads it.
        """
        counter = int((at if at is not None else time.time()) // cls.STEP_SECONDS)
        return cls._hotp(secret, counter)

    @classmethod
    def matched_totp_counter(
        cls, secret: str, code: str, *, window: int = 1, at: float | None = None
    ) -> int | None:
        """The time-step counter ``code`` matches within ±``window``, else None.

        Exposing the COUNTER (not just a boolean) lets the stateful caller
        mark it spent — RFC 6238 §5.2 requires rejecting a second use of the
        same OTP, and the ±window acceptance would otherwise leave every
        accepted code replayable for up to 90 seconds.

        ``at`` (unix seconds) is injectable for tests; production reads the
        wall clock. Comparison is constant-time per candidate so a timing
        side-channel can't leak how close a guess was.
        """
        if not secret or not code:
            return None
        code = code.strip().replace(" ", "")
        if len(code) != cls.DIGITS or not code.isdigit():
            return None
        try:
            counter = int((at if at is not None else time.time()) // cls.STEP_SECONDS)
            for drift in range(-window, window + 1):
                candidate = counter + drift
                # The counter is packed as an unsigned 64-bit int, so a
                # negative candidate raises rather than simply failing to
                # match. Skip it instead of letting one unusable neighbour
                # abort the whole window — a check must fail on the CODE,
                # never on the clock.
                if candidate < 0:
                    continue
                if hmac.compare_digest(cls._hotp(secret, candidate), code):
                    return candidate
        except Exception:
            return None
        return None

    @classmethod
    def verify_totp(
        cls, secret: str, code: str, *, window: int = 1, at: float | None = None
    ) -> bool:
        """True when ``code`` is a valid TOTP for ``secret`` within ±``window`` steps.

        Stateless check only — login-grade acceptance must go through a
        caller's single-use guard built on ``matched_totp_counter``.
        """
        return cls.matched_totp_counter(secret, code, window=window, at=at) is not None

    @classmethod
    def provisioning_uri(cls, secret: str, *, account: str, issuer: str) -> str:
        """otpauth:// URI the enroll response renders as a QR code."""
        label = quote(f"{issuer}:{account}", safe="")
        query = (
            f"secret={secret}"
            f"&issuer={quote(issuer, safe='')}"
            f"&algorithm=SHA1&digits={cls.DIGITS}&period={cls.STEP_SECONDS}"
        )
        return f"otpauth://totp/{label}?{query}"

    @classmethod
    def generate_backup_codes(cls, count: int | None = None) -> list[str]:
        """``count`` plaintext single-use codes for one-time display.

        Grouped ``xxxxx-xxxxx`` for readability; the caller stores only
        hashes (via ``Hash.make``) and matches input through
        ``normalize_backup_code``.
        """
        wanted = cls.BACKUP_CODE_COUNT if count is None else count
        codes = []
        for _ in range(wanted):
            raw = "".join(secrets.choice(cls._BACKUP_ALPHABET) for _ in range(10))
            codes.append(f"{raw[:5]}-{raw[5:]}")
        return codes

    @staticmethod
    def normalize_backup_code(code: str) -> str:
        """Canonical form for hashing/compare — case- and separator-insensitive."""
        return (code or "").strip().lower().replace(" ", "").replace("-", "")


__all__ = ["TwoFactor"]
