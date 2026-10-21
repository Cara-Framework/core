"""HTTP response cache-header helpers — opt-in browser + CDN caching.

The Laravel-parity ``cache.headers`` surface, in the imperative form this
framework's controllers use (call a helper on the outgoing ``Response``).

Two tiers, depending on whether the response is user-specific or shareable:

* ``apply_private_cache`` — ``private, max-age=N`` + ``Vary:
  Authorization`` so a shared CDN/proxy never serves a cached copy across
  users and a logout doesn't expose the previous user's payload. Use for
  endpoints surfacing auth-derived hints.
* ``apply_public_swr_cache`` — ``public, s-maxage=…,
  stale-while-revalidate=…`` so CDN edges serve a single copy across
  users and refresh it in the background (no cache-miss latency cliff).
  Use for user-agnostic browse surfaces, sitemaps, feeds.

Never cache write endpoints, auth flows, or ``status: "generating"`` 202
poll stubs — use ``apply_no_cache`` to be explicit there.

``append_vary`` is how a middleware adds the request header its own
transformation depends on, without dropping the tokens other layers added.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cara.http import Response

_PRIVATE_VARY = "Authorization, Accept-Encoding"
_PUBLIC_VARY = "Accept-Encoding"


def apply_private_cache(response: Response, max_age: int) -> Response:
    """Tag with ``private, max-age=N`` + auth-keyed ``Vary``."""
    response.header("Cache-Control", f"private, max-age={int(max_age)}")
    response.header("Vary", _PRIVATE_VARY)
    return response


def apply_public_swr_cache(
    response: Response,
    max_age: int,
    stale_while_revalidate: int,
    s_max_age: int | None = None,
) -> Response:
    """Public cache with ``stale-while-revalidate`` for graceful refresh.

    Typical pairing: ``max_age=60, stale_while_revalidate=300`` means
    "serve fresh for 60s, then serve stale for up to 5 more minutes while
    re-fetching in the background".
    """
    sm = max_age if s_max_age is None else s_max_age
    response.header(
        "Cache-Control",
        f"public, max-age={int(max_age)}, s-maxage={int(sm)}, "
        f"stale-while-revalidate={int(stale_while_revalidate)}",
    )
    response.header("Vary", _PUBLIC_VARY)
    return response


def append_vary(response: Response, token: str) -> Response:
    """Add ``token`` to ``Vary``, keeping every token already there.

    Each layer that makes a response depend on a request header owns one
    token: compression ``Accept-Encoding``, authentication ``Authorization``,
    CORS ``Origin``. Setting the header outright replaced the others —
    ``HandleCors`` unwinds after the inner layers, and its ``Vary: Origin``
    dropped their tokens, so a shared cache could hand gzip bytes to a
    client that never accepted gzip.
    """
    headers = getattr(response, "headers", None)
    existing = headers.get("Vary") if hasattr(headers, "get") else None
    tokens = [part.strip() for part in (existing or "").split(",") if part.strip()]
    if not any(part.lower() == token.lower() for part in tokens):
        tokens.append(token)
    response.header("Vary", ", ".join(tokens))
    return response


def apply_no_cache(response: Response) -> Response:
    """Explicit opt-out. Use for poll endpoints returning 202 stubs."""
    response.header("Cache-Control", "no-store, must-revalidate")
    return response
