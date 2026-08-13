"""CompressResponses — opt-in gzip middleware for HTTP responses.

Most prod deployments terminate compression at the edge (nginx, ALB,
Cloudfront) so the application server never gzips itself. This
middleware is the origin-side fallback for the cases where that
isn't true:

  - direct-origin probes / dev environments without an edge proxy
  - smaller deployments where the operator hasn't wired up a CDN
  - private internal endpoints that bypass the public edge

Behaviour:

  - Only compresses when the client sent ``Accept-Encoding: gzip``.
  - Only compresses when the response is large enough to benefit
    (``min_size``, default 1 KB). Tiny JSON envelopes don't compress
    well — the gzip header overhead can leave them BIGGER.
  - Only compresses compressible content types (text/*, application/
    json, javascript). Refuses to touch already-compressed payloads
    (images, gzip, brotli, video, audio) — re-gzipping a JPEG wastes
    CPU and grows the byte count.
  - Skips when ``Content-Encoding`` is already set (an upstream
    middleware already compressed) or when the response is a
    streaming/SSE body.
  - Sets ``Vary: Accept-Encoding`` so shared caches keyed on the URL
    don't serve gzip bytes to clients that didn't ask for them.

Configurable via ``config/compression.py`` → ``COMPRESSION`` dict:

  - ``enabled``: master switch (default True)
  - ``min_size``: bytes (default 1024)
  - ``level``: 1-9 (default 6 — same as nginx's ``gzip_comp_level``)
  - ``content_types``: list of prefixes that ARE compressible
"""

from __future__ import annotations

import gzip
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from cara.configuration import config
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware.Middleware import Middleware

# Default compressible MIME prefixes. Match conservatively: anything
# already compressed (image/*, video/*, audio/*, application/zip,
# application/gzip, application/x-bzip2, application/x-7z-compressed,
# font/woff2) is excluded by absence, not by an explicit blocklist —
# easier to extend than to maintain a denylist that lags new formats.
_DEFAULT_COMPRESSIBLE_PREFIXES: tuple[str, ...] = (
    "text/",
    "application/json",
    "application/ld+json",
    "application/javascript",
    "application/xml",
    "application/xhtml+xml",
    "application/manifest+json",
    "application/atom+xml",
    "application/rss+xml",
    "application/graphql",
    "application/x-ndjson",
    "image/svg+xml",
)

_DEFAULT_MIN_SIZE = 1024  # 1 KB — below this gzip overhead dominates.
_DEFAULT_LEVEL = 6  # nginx's gzip_comp_level default.


class CompressResponses(Middleware):
    """Gzip-compress eligible responses based on Accept-Encoding."""

    def __init__(self, application, parameters=None):
        super().__init__(application)
        self.parameters = parameters or []
        self._enabled, self._min_size, self._level, self._prefixes = self._load_config()

    @staticmethod
    def _load_config() -> tuple[bool, int, int, tuple[str, ...]]:
        enabled = config("compression.compression.enabled", True)
        min_size = config("compression.compression.min_size", _DEFAULT_MIN_SIZE)
        level = config("compression.compression.level", _DEFAULT_LEVEL)
        configured_types = config("compression.compression.content_types", None)

        if not isinstance(enabled, bool):
            raise TypeError("Response compression enabled must be boolean")
        if type(min_size) is not int or min_size < 0:
            raise ValueError(
                "Response compression min_size must be a non-negative integer"
            )
        if type(level) is not int or not 1 <= level <= 9:
            raise ValueError("Response compression level must be an integer from 1 to 9")
        if configured_types is None:
            prefixes = _DEFAULT_COMPRESSIBLE_PREFIXES
        else:
            if not isinstance(configured_types, (list, tuple)) or not configured_types:
                raise TypeError(
                    "Response compression content_types must be a non-empty list"
                )
            if any(
                not isinstance(value, str) or not value.strip()
                for value in configured_types
            ):
                raise ValueError(
                    "Response compression content_types must contain non-empty strings"
                )
            prefixes = tuple(value.strip().lower() for value in configured_types)
        return enabled, min_size, level, prefixes

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        response = await next_fn(request)

        if not self._enabled:
            return response

        try:
            if not self._accepts_gzip(request):
                return response
            if self._already_encoded(response):
                return response
            content_type = self._response_content_type(response)
            if not self._is_compressible_type(content_type):
                return response
            body = self._response_bytes(response)
            if body is None or len(body) < self._min_size:
                return response

            compressed = gzip.compress(body, compresslevel=self._level)
            # Defensive: if gzip somehow grew the payload (very short or
            # incompressible content that slipped past the type filter),
            # send the original. Re-checking here keeps a malformed
            # client + small payload from going on the wire as a
            # larger gzip blob.
            if len(compressed) >= len(body):
                return response

            # Commit headers before replacing the body. If a custom response
            # cannot accept the compression headers, the outer handler keeps
            # the original bytes intact instead of returning compressed bytes
            # without a correct cache key.
            self._append_vary(response, "Accept-Encoding")
            response.header("Content-Encoding", "gzip")
            response.header("Content-Length", str(len(compressed)))
            response.content = compressed
        except Exception as e:
            # Never break a response because compression failed — fall
            # through with the original bytes. Log at debug so a
            # systematic issue is visible during incident review.
            self._log_debug(
                f"CompressResponses: gzip failed ({e.__class__.__name__}: {e})"
            )

        return response

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _accepts_gzip(request: Request) -> bool:
        try:
            raw = request.header("Accept-Encoding")
        except Exception:
            raw = None
        if not raw or not isinstance(raw, str):
            return False
        # Token match — ``Accept-Encoding`` values can include weights
        # (``gzip;q=0``). Refuse to compress when the client explicitly
        # weights gzip at zero.
        for token in raw.split(","):
            tok = token.strip().lower()
            if not tok.startswith("gzip"):
                continue
            return not (";q=0" in tok and ";q=0." not in tok)
        return False

    @staticmethod
    def _already_encoded(response: Any) -> bool:
        try:
            existing = None
            headers = getattr(response, "headers", None)
            if headers is not None and hasattr(headers, "get"):
                existing = headers.get("Content-Encoding")
            if not existing:
                # Fallback: scan the header bag directly. ``all()``
                # returns a dict — iterating it bare yields key strings,
                # and the tuple-unpack ValueError killed EVERY
                # compression attempt via handle()'s broad except.
                bag = getattr(response, "header_bag", None)
                if bag is not None and hasattr(bag, "all"):
                    for k, v in (bag.all() or {}).items():
                        if str(k).lower() == "content-encoding" and v:
                            existing = v
                            break
            return bool(existing) and str(existing).strip().lower() != "identity"
        except AttributeError, TypeError, RuntimeError:
            return False

    @staticmethod
    def _response_content_type(response: Any) -> str:
        try:
            headers = getattr(response, "headers", None)
            if headers is not None and hasattr(headers, "get"):
                ct = headers.get("Content-Type")
                if ct:
                    return str(ct).split(";", 1)[0].strip().lower()
        except OSError, RuntimeError, AttributeError, ConnectionError:
            pass
        return ""

    def _is_compressible_type(self, content_type: str) -> bool:
        if not content_type:
            return False
        return any(content_type.startswith(prefix) for prefix in self._prefixes)

    @staticmethod
    def _response_bytes(response: Any) -> bytes | None:
        try:
            content = getattr(response, "content", None)
            if isinstance(content, bytes):
                return content
            if isinstance(content, str):
                return content.encode("utf-8")
            if content is None:
                return None
            # Streaming / generator bodies — refuse. Compressing a
            # streaming response would buffer the whole payload, which
            # defeats the streaming and risks OOM on large feeds.
            if isinstance(content, (Iterable,)) and not isinstance(content, (bytes, str)):
                return None
        except AttributeError, TypeError, RuntimeError:
            return None
        return None

    @staticmethod
    def _append_vary(response: Any, value: str) -> None:
        existing = ""
        headers = getattr(response, "headers", None)
        if headers is not None and hasattr(headers, "get"):
            existing = headers.get("Vary") or ""
        tokens = [token.strip() for token in existing.split(",") if token.strip()]
        if not any(token.lower() == value.lower() for token in tokens):
            tokens.append(value)
        response.header("Vary", ", ".join(tokens))

    @staticmethod
    def _log_debug(msg: str) -> None:
        """Debug log for a swallowed compression failure.

        Previously a function-local ``from cara.facades import Log`` inside a
        ``try`` whose ``except`` re-imported and re-called the SAME facade —
        a handler that cannot succeed where the body failed. On the response
        path that meant a logging helper raising out of ``handle`` and turning
        a servable response into a 500. Partial-boot tolerance already has one
        source: ``Facade.__getattr__``'s ``cls.key == "logger"`` fallback.
        """
        Log.debug(msg, category="cara.http.compress_responses")
