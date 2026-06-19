"""Per-IP rate limit on WebSocket handshakes.

Sibling of the HTTP ``ThrottleRequests`` middleware. HTTP routes have
named throttles wired through the ``throttle:<name>`` alias; public
WebSocket upgrades had no equivalent until this middleware. A client
loop-connecting to ``/ws/deals`` could open hundreds of sockets per
second and starve the connection pool, the broadcaster's per-channel
fan-out, and (downstream) the Redis pub/sub subscriber count — no
HTTP throttle middleware reaches the upgrade because the request
never crosses the HTTP capsule.

Opt-in: routes that want the protection declare
``middleware=["ws.throttle"]`` (or ``"ws.throttle:<name>"`` to target
a named bucket). The alias is registered in
:meth:`MiddlewareProvider._register_core_ws_aliases`.

The implementation mirrors ``ThrottleRequests`` for parity:

  * IP resolution is gated on the same trusted-proxy CIDR
    (``_is_trusted_proxy`` from ``cara.http.request.Request``) — an
    attacker who can set ``X-Forwarded-For`` but isn't behind a
    trusted proxy is still bucketed under their real peer IP.
  * The bucket key includes the resolved route path so different
    public channels (``/ws/deals`` vs ``/ws/live/products``) don't
    starve each other.
  * Per-bucket limits come from ``rate.<name>`` config keys
    (e.g. ``rate.ws_connect``) so ops can tune without a code change.
  * Cache failure defaults to fail-CLOSED (reject the handshake) —
    mirrors the HTTP throttle's ``_DEFAULT_FAIL_OPEN = False``
    contract in ``ThrottleRequests``. Operators who want the legacy
    fail-open posture can flip ``rate.fail_open`` in config.
  * Limit-exceeded rejects with WebSocket close code 4008, which
    the framework already documents in
    :class:`cara.exceptions.types.websocket.WebSocketException` as
    "Rate limit exceeded". Sitting in the 4000-4999 application-
    specific range keeps it from colliding with the protocol-level
    1008 (policy violation) so clients can branch on "throttled"
    cleanly. The browser surfaces it as a standard close event so
    the consumer's reconnect-backoff kicks in without an error path.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cara.configuration import config
from cara.exceptions.types.websocket import WebSocketException
from cara.facades import Log
from cara.http.request.Request import _is_trusted_proxy
from cara.middleware import Middleware
from cara.support.Str import mask_ip
from cara.websocket import Socket

_RATE_LIMIT_CLOSE_CODE = 4008  # WebSocketException docs: "Rate limit exceeded"
_DEFAULT_FAIL_OPEN = False
_DEFAULT_LIMIT_PER_MINUTE = 30
_DEFAULT_WINDOW_SECONDS = 60


class Throttle(Middleware):
    """Per-IP, per-channel rolling-window rate limit on WebSocket
    handshakes. Mirrors the contract of the HTTP ``ThrottleRequests``
    middleware so the same ops dashboards / alerts can cover both
    transports."""

    def __init__(self, application, name: str = "ws_connect"):
        super().__init__(application)
        self.name = name

    async def handle(self, socket: Socket, next_fn: Callable):
        limit, window = self._limits()
        ip = self._client_ip(socket)
        path = self._path(socket)
        key = f"throttle:ws:{self.name}:{ip}:{path}"

        # Shared counting + Redis-down fallback via the same helper the
        # HTTP throttle uses so both transports honour
        # ``rate.fallback_mode`` / ``rate.fail_open`` uniformly.
        from cara.exceptions import ServiceUnavailableException
        from cara.rates.MemoryRateStore import attempt_with_fallback

        try:
            allowed, _remaining, _reset_in, _backend = attempt_with_fallback(
                cache_key=key,
                window_seconds=window,
                max_attempts=limit,
            )
        except ServiceUnavailableException as e:
            Log.warning("WebSocket throttle cache failure for key %s; failing closed. %s", key, e, category='cara.websocket')
            try:
                await socket.send(
                    {
                        "type": "websocket.close",
                        "code": _RATE_LIMIT_CLOSE_CODE,
                    }
                )
            except (OSError, RuntimeError, AttributeError, ConnectionError):
                pass
            raise WebSocketException(
                "WebSocket rate limiter temporarily unavailable",
                _RATE_LIMIT_CLOSE_CODE,
            ) from e

        if not allowed:
            Log.warning("WebSocket throttle exceeded: ip=%s path=%s name=%s limit=%s", mask_ip(ip), path, self.name, limit, category='cara.websocket')
            try:
                await socket.send(
                    {
                        "type": "websocket.close",
                        "code": _RATE_LIMIT_CLOSE_CODE,
                    }
                )
            except (OSError, RuntimeError, AttributeError, ConnectionError):
                pass
            raise WebSocketException(
                f"WebSocket connect rate exceeded (>{limit} per {window}s)",
                _RATE_LIMIT_CLOSE_CODE,
            )

        return await next_fn(socket)

    def _fail_open_mode(self) -> bool:
        """Whether to allow handshakes when the cache backend is down.

        Default False (fail-closed). Mirrors ``ThrottleRequests`` so
        operators flip ``rate.fail_open`` in config when availability
        outweighs abuse protection.
        """
        try:
            from cara.facades import Config

            return bool(Config.get("rate.fail_open", _DEFAULT_FAIL_OPEN))
        except Exception:
            return _DEFAULT_FAIL_OPEN

    def _limits(self) -> tuple[int, int]:
        """Read per-named-throttle limit + window from config.

        ``rate.ws_connect.limit`` / ``rate.ws_connect.window`` are
        the canonical knobs; falls back to the module defaults if the
        env hasn't set them so the middleware is safe to enable
        without a config change.
        """
        try:
            limit = int(config(f"rate.{self.name}.limit", _DEFAULT_LIMIT_PER_MINUTE))
        except (TypeError, ValueError):
            limit = _DEFAULT_LIMIT_PER_MINUTE
        try:
            window = int(config(f"rate.{self.name}.window", _DEFAULT_WINDOW_SECONDS))
        except (TypeError, ValueError):
            window = _DEFAULT_WINDOW_SECONDS
        return max(1, limit), max(1, window)

    @staticmethod
    def _client_ip(socket: Socket) -> str:
        """Resolve client IP with the same trusted-proxy gate the
        HTTP request uses.

        ASGI scope's ``client`` is a ``(host, port)`` tuple — the
        immediate peer. If that peer is in the configured trusted-
        proxy CIDR (``TRUSTED_PROXIES`` env / ``app.TRUSTED_PROXIES``
        config), walk left through ``X-Forwarded-For`` to the first
        non-trusted hop. Otherwise pin to the peer IP — an attacker
        who can set the header but isn't behind a trusted proxy must
        not control their own bucket key.
        """
        scope: dict[str, Any] = socket.scope or {}
        client = scope.get("client") or (None, None)
        peer_ip = client[0] if client and client[0] else ""

        if peer_ip and _is_trusted_proxy(peer_ip):
            fwd_header = ""
            for k, v in scope.get("headers", []):
                if k == b"x-forwarded-for":
                    fwd_header = v.decode("latin-1", errors="replace")
                    break
            if fwd_header:
                # Right-to-left walk picks the closest non-trusted hop.
                for candidate in (h.strip() for h in reversed(fwd_header.split(","))):
                    if not candidate:
                        continue
                    if not _is_trusted_proxy(candidate):
                        return candidate
        return peer_ip or "unknown"

    @staticmethod
    def _path(scope_or_socket: Socket) -> str:
        """ASGI scope path — included in the bucket key so a single
        public channel can't drain another channel's quota."""
        try:
            return str(scope_or_socket.scope.get("path") or "/")
        except Exception:
            return "/"
