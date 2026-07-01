"""ConditionalGet — opt-in ETag / conditional-GET middleware.

Computes a **weak** ``ETag`` from the serialized response body for safe
GET (and HEAD) requests that produced a 2xx body, and answers a matching
``If-None-Match`` with a bodyless ``304 Not Modified`` — saving the
bytes on the wire while letting the client reuse its cached copy.

Why weak ETags
--------------
A weak validator (``W/"…"``) asserts *semantic* equivalence, not
byte-for-byte identity. That is exactly the contract we can honour here:
the tag is derived from the body the application already serialized, but
a downstream layer (gzip in :class:`CompressResponses`, an edge proxy)
may re-encode the bytes. Weak comparison — the only comparison
``If-None-Match`` is allowed to use per RFC 7232 §3.2 / §2.3.2 — treats
two representations as equal when their opaque-tags match regardless of
the ``W/`` weakness flag, so a gzip-vs-identity difference still yields
a correct 304.

Design
------
* **Opt-in, per route.** This is a named middleware. Apps register it
  as an alias (``registry.alias("etag", ConditionalGet)``) and attach it
  to the routes that benefit (``Route.get(..., middleware=["etag"])``).
  It is never global.
* **Safe methods only.** Acts on ``GET``/``HEAD``. Anything mutating
  (POST/PUT/PATCH/DELETE) passes straight through untouched.
* **2xx bodies only.** A 3xx/4xx/5xx — or a 204/304/streaming response
  with no concrete byte body — is returned unchanged. Re-validating an
  error body with an ETag is meaningless.
* **Does not fight cache-control.** The middleware only ever sets
  ``ETag`` (and, on a 304, prunes the body/length headers). It never
  writes ``Cache-Control`` / ``Expires`` / ``Vary`` — those stay owned
  by the controller's ``apply_*_cache`` helpers / :class:`HeaderManager`.
* **Never breaks a response.** Any failure while hashing or comparing
  falls through with the original response and a debug log, mirroring
  :class:`CompressResponses` / :class:`SecurityHeaders`.
"""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from cara.http import Request, Response
from cara.middleware import Middleware

# Methods for which a body may be safely cache-validated. HEAD shares
# GET semantics (same representation, empty body) so an ETag is still
# meaningful — a HEAD with a matching If-None-Match collapses to 304.
_SAFE_METHODS: frozenset[str] = frozenset({"GET", "HEAD"})


class ConditionalGet(Middleware):
    """Emit weak ETags and answer ``If-None-Match`` with ``304``."""

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        response = await next_fn(request)

        try:
            if not self._is_safe_method(request):
                return response
            if not self._is_2xx(response):
                return response

            body = self._response_bytes(response)
            if body is None:
                # Streaming / generator / absent body — nothing stable to
                # hash. Leave the response untouched.
                return response

            etag = self._compute_weak_etag(body)
            # Always advertise the validator on the full response so the
            # *next* request can send If-None-Match.
            response.header("ETag", etag)

            inm = self._request_header(request, "If-None-Match")
            if inm and self._if_none_match_matches(inm, etag):
                self._make_not_modified(response, etag)
        except Exception as e:
            # A conditional-GET optimisation must never break the actual
            # response. Fall through with whatever we have.
            self._log_debug(
                f"ConditionalGet: failed ({e.__class__.__name__}: {e})"
            )

        return response

    # ── Method / status gates ────────────────────────────────────────

    @staticmethod
    def _is_safe_method(request: Request) -> bool:
        try:
            method = getattr(request, "method", None)
            if not isinstance(method, str):
                return False
            return method.upper() in _SAFE_METHODS
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def _is_2xx(response: Any) -> bool:
        try:
            code = getattr(response, "status_code", None)
            if code is None and hasattr(response, "get_status_code"):
                code = response.get_status_code()
            return isinstance(code, int) and 200 <= code < 300
        except (AttributeError, TypeError):
            return False

    # ── Body access ──────────────────────────────────────────────────

    @staticmethod
    def _response_bytes(response: Any) -> bytes | None:
        """Return the concrete byte body, or ``None`` for streaming/empty.

        Mirrors :meth:`CompressResponses._response_bytes`: a streaming
        generator body is refused (hashing it would buffer the whole
        feed and defeat streaming), and ``None``/absent content yields
        ``None`` so the caller skips ETag emission entirely.
        """
        try:
            content = getattr(response, "content", None)
            if isinstance(content, bytes):
                return content
            if isinstance(content, str):
                return content.encode("utf-8")
            if content is None:
                return None
            # Streaming / iterable body — refuse.
            if isinstance(content, Iterable):
                return None
        except (AttributeError, TypeError, RuntimeError):
            return None
        return None

    # ── ETag computation + comparison ────────────────────────────────

    @staticmethod
    def _compute_weak_etag(body: bytes) -> str:
        """Build a weak ETag (``W/"<hex>"``) from a stable body hash.

        SHA-256 keeps collisions astronomically unlikely; the digest is
        truncated to 32 hex chars (128 bits) to keep the header small.
        """
        digest = hashlib.sha256(body).hexdigest()[:32]
        return f'W/"{digest}"'

    @classmethod
    def _if_none_match_matches(cls, header_value: str, etag: str) -> bool:
        """RFC 7232 weak comparison of ``If-None-Match`` against ``etag``.

        * ``*`` matches any current representation.
        * Otherwise each comma-separated candidate is compared by its
          opaque-tag only — the ``W/`` weakness flag is ignored on both
          sides (weak comparison), which is what ``If-None-Match`` always
          uses.
        """
        value = header_value.strip()
        if value == "*":
            return True

        target = cls._opaque_tag(etag)
        if target is None:
            return False

        for candidate in value.split(","):
            cand = candidate.strip()
            if not cand:
                continue
            if cls._opaque_tag(cand) == target:
                return True
        return False

    @staticmethod
    def _opaque_tag(raw: str) -> str | None:
        """Strip the optional ``W/`` weak prefix and surrounding quotes.

        Returns the inner opaque-tag (the bytes between the quotes), or
        ``None`` when the token isn't a quoted entity-tag.
        """
        token = raw.strip()
        if token[:2] in ("W/", "w/"):
            token = token[2:].strip()
        if len(token) >= 2 and token[0] == '"' and token[-1] == '"':
            return token[1:-1]
        return None

    # ── 304 transformation ───────────────────────────────────────────

    @staticmethod
    def _make_not_modified(response: Any, etag: str) -> None:
        """Collapse ``response`` to a bodyless ``304 Not Modified``.

        Preserves the ``ETag`` (and any cache-control / vary headers the
        controller set) while dropping the body and the now-misleading
        ``Content-Length`` / ``Content-Type``. RFC 7232 §4.1: a 304 must
        not carry a message body and should omit the representation
        headers that would describe one.
        """
        # Empty the body. ``set_content`` keeps the bytes invariant the
        # rest of the response pipeline relies on.
        if hasattr(response, "set_content"):
            response.set_content(b"")
        else:
            response.content = b""

        # 304.
        if hasattr(response, "status"):
            response.status(304)
        else:
            response._status = 304

        # Re-assert the validator (it survived from the full response, but
        # be explicit so a 304 is never emitted without its ETag).
        response.header("ETag", etag)

        # Drop representation headers that no longer describe a body.
        headers = getattr(response, "headers", None)
        if headers is not None and hasattr(headers, "remove"):
            for name in ("Content-Length", "Content-Type"):
                with contextlib.suppress(AttributeError, KeyError, TypeError):
                    headers.remove(name)

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _request_header(request: Request, name: str) -> str | None:
        try:
            value = request.header(name)
        except Exception:
            return None
        return value if isinstance(value, str) else None

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Best-effort debug log; survives partial-boot."""
        try:
            from cara.facades import Log

            Log.debug(msg, category="cara.http.conditional_get")
        except Exception:
            from cara.facades import Log

            Log.warning(msg, exc_info=True)
