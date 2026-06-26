"""Block requests whose ``User-Agent`` matches a configured deny-list.

Defence-in-depth against trivially-attributable automated traffic
(generic ``curl``, ``wget``, ``python-requests`` user-agents). The
existing rate-limit middleware already caps per-IP traffic, but a
distributed crawler complying with the per-IP ceiling can still
harvest public data over time. Rejecting obvious automated-client
UAs at the door is cheap and pushes the attacker into either
spoofing a real browser UA (slower / harder to script at scale) or
giving up.

Policy:
  * ``config("security.blocked_user_agents")`` — list of case-
    insensitive substrings. Default: ``[]`` (no-op).
  * When the request's UA contains any blocked substring → 403 with
    the canonical ``{"error", "type"}`` middleware envelope (same
    shape ``ThrottleRequests`` / ``EnforceBodySizeLimit`` /
    ``CheckMaintenanceMode`` / ``CanPerform`` / ``ShouldAuthenticate``
    emit). The previous body — ``{"errors": {"__all__": [...]}}`` —
    was the validation-error shape Cara uses for 422s and forced
    API consumers to substring-match the human
    "Forbidden user-agent" string because the ``type`` discriminator
    every other middleware path carries was missing.
  * Missing UA header: NOT blocked. Some legitimate clients (older
    mobile webviews, internal health probes) omit the header; pairing
    "missing UA → block" with the IP allow-list middleware would
    accidentally lock out monitoring agents.

The check is a plain ``substring in lower(ua)`` rather than a regex
to keep the hot path cheap (every public request runs this) and to
keep the env-var format forgiving — operators paste tokens, not
regular expressions.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable

from cara.configuration import config
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware import Middleware
from cara.middleware.http.HandleCors import apply_cors_headers_to_response


class FilterBlockedUserAgents(Middleware):
    """403 when the request's User-Agent matches a configured token."""

    async def handle(
        self,
        request: Request,
        next_fn: Callable[..., Awaitable[Response]],
    ) -> Response:
        tokens = list(self._iter_tokens(config("security.blocked_user_agents", [])))
        if not tokens:
            # Empty allow-list → no-op. Middleware can sit in the global
            # chain without changing behaviour until ops populates the
            # env var.
            return await next_fn(request)

        ua = self._user_agent(request)
        if not ua:
            return await next_fn(request)

        ua_lower = ua.lower()
        for token in tokens:
            if token and token in ua_lower:
                Log.warning(
                    f"FilterBlockedUserAgents: rejected user_agent={ua!r} "
                    f"matched token={token!r}",
                    category="security.user_agent",
                )
                response = Response(self.application).json(
                    {
                        "error": "Forbidden user-agent",
                        "type": "forbidden_user_agent",
                    },
                    403,
                )
                # Position 4 in the global chain — ``HandleCors`` at
                # position 9 never runs when we return here. Stamp
                # CORS headers explicitly so a legitimate JS client
                # that happens to send a blocked UA (e.g. a Postman
                # dev probe from the client's local dev origin)
                # sees the 403 instead of an opaque CORS error.
                apply_cors_headers_to_response(
                    self.application,
                    request,
                    response,
                )
                return response
        return await next_fn(request)

    @staticmethod
    def _iter_tokens(raw: str | Iterable[str] | None) -> Iterable[str]:
        """Normalise list / comma-string / mixed into lowercase tokens."""
        if isinstance(raw, str):
            for token in raw.split(","):
                t = token.strip().lower()
                if t:
                    yield t
            return
        for entry in raw or []:
            if not isinstance(entry, str):
                continue
            for token in entry.split(","):
                t = token.strip().lower()
                if t:
                    yield t

    @staticmethod
    def _user_agent(request: Request) -> str:
        """Best-effort UA header read across Cara request adapters."""
        try:
            getter = getattr(request, "header", None)
            if callable(getter):
                val = getter("User-Agent") or getter("user-agent")
                if isinstance(val, str):
                    return val
        except Exception as e:
            # ``Cara`` adapters surface header access differently across
            # transports (raw scope dict vs `Request.header()`); a getter
            # crashing is a "no UA on this request" — never the request's
            # fault. Demoted from silent swallow to ``debug`` so the
            # convention check ("never bare ``except`` without logging")
            # is satisfied without polluting prod logs at warn level.
            Log.debug(
                f"FilterBlockedUserAgents: header(...) accessor failed: {e}",
                category="security.user_agent",
            )
        try:
            headers = getattr(request, "headers", None)
            if headers is not None:
                for key in ("User-Agent", "user-agent"):
                    val = headers.get(key) if hasattr(headers, "get") else None
                    if isinstance(val, str):
                        return val
        except Exception as e:
            Log.debug(
                f"FilterBlockedUserAgents: headers.get(...) accessor failed: {e}",
                category="security.user_agent",
            )
        return ""
