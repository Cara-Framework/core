"""One request's claim on its ``Idempotency-Key`` (Stripe's contract).

The first request with a key runs and its answer is kept for 24 h; a
replay with the same request answers that stored result without running
again; the same key with a different request is a 422; a replay while the
first still runs is a 409. The caller decides the scope of a key (the
``ReplayIdempotentRequests`` middleware: the caller's identity and the
route), so two integrations never collide on a key.

The claim fails CLOSED. It is held for the whole replay window, so a lost
``done`` write never reopens the key to a second run; a claim still
``running`` past ``CLAIM_SECONDS`` is a first run whose outcome is unknown
and answers 409 "check, then send a new key", never a re-run. A cache that
cannot be reached while claiming is a 503: nothing ran, so the caller
retries with the same key.

The store is the ``Cache`` facade (``add`` is the atomic claim; Redis
``SET NX``); ``STORE`` names a cache driver when the product keeps keys
apart from its disposable cache.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from cara.cache import cache_backend_fault_types
from cara.exceptions import (
    IdempotencyKeyConflictException,
    ServiceUnavailableException,
    ValidationException,
)
from cara.facades import Cache, Log
from cara.http.request.Request import Request

#: Stashed on the request by the middleware, so the door can say its
#: effect landed (``IdempotencyClaim.landed``).
_REQUEST_ATTR = "_cara_idempotency_claim"


class IdempotencyClaim:
    """A held key (or none: every method is then a no-op)."""

    HEADER = "Idempotency-Key"
    MAX_KEY_LENGTH = 255
    CLAIM_SECONDS = 300
    KEEP_SECONDS = 86_400
    #: Cache driver holding the claims; None is the default store.
    STORE: str | None = None
    #: What a refused key says; a door with its own subject words its own.
    RUNNING = "A request with this Idempotency-Key is still running. Retry shortly."
    REUSED = "This Idempotency-Key was already used with a different request."
    UNKNOWN = (
        "The first request with this Idempotency-Key may have taken effect, but "
        "its result was not recorded. Check the result, then send a new "
        "Idempotency-Key if it is still needed."
    )
    UNAVAILABLE = (
        "Idempotency-Key could not be checked right now; nothing was done. "
        "Retry with the same Idempotency-Key."
    )

    def __init__(
        self,
        cache_key: str | None,
        fingerprint: str,
        *,
        category: str = "http.idempotency",
    ) -> None:
        self.cache_key = cache_key
        self.fingerprint = fingerprint
        self.category = category
        self.answer: dict | None = None
        # Set by the door once its effect committed: a failure after that
        # point is an unknown outcome, never a free key.
        self.effect_landed = False

    # ── The request side ─────────────────────────────────────────────

    @classmethod
    def request_key(cls, request: Request) -> str | None:
        """The request's key, or None; an oversized key is a 422."""
        raw = str(request.header(cls.HEADER) or "").strip()
        if not raw:
            return None
        if len(raw) > cls.MAX_KEY_LENGTH:
            raise ValidationException.field(
                cls.HEADER,
                f"{cls.HEADER} cannot exceed {cls.MAX_KEY_LENGTH} characters.",
            )
        return raw

    @staticmethod
    def digest(value: Any) -> str:
        """SHA-256 of ``value`` as canonical JSON (sorted keys, no spaces)."""
        blob = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def attach(self, request: Request) -> None:
        setattr(request, _REQUEST_ATTR, self)

    @staticmethod
    def landed(request: Request) -> None:
        """The door's effect committed. Call it right after the use case
        returns: a failure while shaping the answer then keeps the key
        closed instead of letting a retry run the effect twice. A no-op on
        a request that holds no key."""
        claim = getattr(request, _REQUEST_ATTR, None)
        if claim is not None:
            claim.effect_landed = True

    # ── The ledger side (sync: call from a worker thread) ────────────

    @classmethod
    def claim(
        cls,
        cache_key: str,
        fingerprint: str,
        *,
        category: str = "http.idempotency",
        running: str = RUNNING,
        reused: str = REUSED,
        unknown: str = UNKNOWN,
    ) -> IdempotencyClaim:
        """Claim the key, or carry the stored answer of its first run
        (``answer``); refuses a reused or still-running key."""
        held_claim = cls(cache_key, fingerprint, category=category)
        try:
            # Claimed for the whole replay window, not CLAIM_SECONDS: an
            # expiring claim reopened the key 5 min after a lost ``done``
            # write, and the retry ran the effect twice.
            if Cache.add(
                cache_key,
                {"fp": fingerprint, "state": "running", "since": int(time.time())},
                cls.KEEP_SECONDS,
                cls.STORE,
            ):
                return held_claim
            held = Cache.get(cache_key, None, cls.STORE) or {}
        except cache_backend_fault_types() as exc:
            # Fail closed: an unchecked key never runs the door.
            Log.error(
                "Idempotency-Key not claimed for %s: %s",
                cache_key,
                exc,
                category=category,
            )
            raise ServiceUnavailableException(cls.UNAVAILABLE, retry_after=5) from exc
        if not held:
            # The claim was released between the two reads: a retry claims it.
            raise IdempotencyKeyConflictException(running)
        if held.get("fp") != fingerprint:
            raise ValidationException.field(cls.HEADER, reused)
        if held.get("state") != "done":
            since = held.get("since")
            if since is not None and int(time.time()) - int(since) > cls.CLAIM_SECONDS:
                raise IdempotencyKeyConflictException(unknown)
            raise IdempotencyKeyConflictException(running)
        held_claim.answer = dict(held["answer"])
        return held_claim

    def keep(self, answer: dict) -> None:
        """Store the answer of a run whose effect committed. A failed write
        leaves the claim ``running`` (a retry is a 409), never raises over
        an effect that already happened."""
        if self.cache_key is None:
            return
        try:
            Cache.put(
                self.cache_key,
                {"fp": self.fingerprint, "state": "done", "answer": answer},
                self.KEEP_SECONDS,
                self.STORE,
            )
        except Exception as exc:
            Log.error(
                "Idempotent answer not stored for %s: %s",
                self.cache_key,
                exc,
                category=self.category,
            )

    def release(self) -> None:
        """A refused or failed request changed nothing: the key may run
        again. A failed release keeps the key closed (a 409), never raises
        over the door's own error."""
        if self.cache_key is None:
            return
        try:
            Cache.forget(self.cache_key, self.STORE)
        except Exception as exc:
            Log.warning(
                "Idempotency key not released for %s: %s",
                self.cache_key,
                exc,
                category=self.category,
            )


__all__ = ["IdempotencyClaim"]
