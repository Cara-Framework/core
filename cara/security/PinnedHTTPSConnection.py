"""Pinned HTTPS connection to an address the SSRF gate already approved.

Validating a URL and then handing it to an HTTP client re-resolves DNS
at connect time — a race the attacker controls. This module connects to
one of the addresses :func:`~cara.security.OutboundUrl.resolve_outbound_url`
returned, while TLS still verifies (and SNIs) the original hostname.

Every redirect is re-parsed and re-resolved through the same gate, and
content type, advertised size and streamed size are all bounded before
the bytes reach the caller.
"""

from __future__ import annotations

import http.client
import socket
import ssl
from collections.abc import Iterable, Mapping
from urllib.parse import urljoin, urlsplit

from cara.security.OutboundUrl import resolve_outbound_url
from cara.security.UnsafeOutboundUrl import UnsafeOutboundUrl

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection whose TCP peer is one already-validated DNS answer."""

    def __init__(
        self,
        hostname: str,
        address: str,
        *,
        port: int = 443,
        connect_timeout: float = 10,
        read_timeout: float = 60,
    ) -> None:
        super().__init__(
            hostname,
            port=port,
            timeout=float(connect_timeout),
            context=ssl.create_default_context(),
        )
        self._pinned_address = address
        self._read_timeout = float(read_timeout)

    def connect(self) -> None:
        # The destination is an IP literal, so ``create_connection`` performs
        # no attacker-raceable second DNS lookup. TLS still verifies/SNIs the
        # original hostname.
        raw = socket.create_connection(
            (self._pinned_address, self.port),
            timeout=self.timeout,
            source_address=self.source_address,
        )
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
            self.sock.settimeout(self._read_timeout)
        except Exception:
            raw.close()
            raise


def open_pinned_https(
    url: str,
    hostname: str,
    address: str,
    *,
    connect_timeout: float,
    read_timeout: float,
    headers: Mapping[str, str],
):
    """Open a pinned GET; split out so transport behavior is unit-testable."""
    parsed = urlsplit(url)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    connection = PinnedHTTPSConnection(
        hostname,
        address,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    try:
        connection.request("GET", target, headers=dict(headers))
        return connection, connection.getresponse()
    except Exception:
        connection.close()
        raise


def download_public_https(
    url: object,
    *,
    max_bytes: int,
    allowed_content_types: Iterable[str],
    user_agent: str,
    label: str = "outbound resource",
    max_redirects: int = 3,
    connect_timeout: float = 10,
    read_timeout: float = 60,
    headers: Mapping[str, str] | None = None,
) -> bytes:
    """Download an untrusted URL through one fail-closed policy.

    ``user_agent`` is required: the framework has no product identity to
    put on the wire, and an anonymous or misattributed agent string is a
    support problem for whoever receives the request.
    """
    limit = int(max_bytes)
    if limit <= 0:
        raise ValueError("download limit must be positive")
    allowed = {
        str(content_type).strip().lower()
        for content_type in allowed_content_types
        if content_type
    }
    if not allowed:
        raise ValueError("at least one content type must be allowed")
    request_headers = {
        "Accept": ", ".join(sorted(allowed)),
        "User-Agent": str(user_agent),
        **dict(headers or {}),
    }

    current = str(url or "").strip()
    for redirect_count in range(int(max_redirects) + 1):
        safe_url, hostname, addresses = resolve_outbound_url(
            current, label=label, allowed_schemes=("https",), allowed_ports=(443,)
        )
        last_connect_error: Exception | None = None
        connection = response = None
        for address in addresses:
            try:
                connection, response = open_pinned_https(
                    safe_url,
                    hostname,
                    address,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    headers=request_headers,
                )
                break
            except Exception as exc:
                last_connect_error = exc
        if response is None or connection is None:
            raise UnsafeOutboundUrl(
                f"{label} could not be downloaded"
            ) from last_connect_error
        try:
            status = int(response.status)
            if status in _REDIRECT_STATUSES:
                location = response.headers.get("Location")
                if not location:
                    raise ValueError(f"{label} redirect has no Location")
                if redirect_count >= int(max_redirects):
                    raise ValueError(f"{label} redirected too many times")
                current = urljoin(safe_url, str(location))
                continue
            if status != 200:
                raise ValueError(f"{label} download failed ({status})")

            length = response.headers.get("Content-Length")
            if length is not None:
                try:
                    advertised = int(length)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{label} returned invalid Content-Length") from exc
                if advertised < 0 or advertised > limit:
                    raise ValueError(f"{label} exceeds the {limit}-byte limit")
            content_type = str(response.headers.get("Content-Type") or "")
            content_type = content_type.split(";", 1)[0].strip().lower()
            if content_type not in allowed:
                raise ValueError(f"{label} returned unsupported content type")

            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = response.read(min(1024 * 1024, limit - total + 1))
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    raise ValueError(f"{label} exceeds the {limit}-byte limit")
                chunks.append(chunk)
            if not chunks:
                raise ValueError(f"{label} is empty")
            return b"".join(chunks)
        finally:
            response.close()
            connection.close()

    raise ValueError(f"{label} redirected too many times")


__all__ = ["PinnedHTTPSConnection", "download_public_https", "open_pinned_https"]
