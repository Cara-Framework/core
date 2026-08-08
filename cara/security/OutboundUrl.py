"""One fail-closed gate for every URL the app opens a socket to.

Any URL that reaches an outbound request and did not come from the
operator is an SSRF primitive: a stored webhook endpoint, a scraped
image, a push subscription, a tenant-configured callback. Pointed at
``169.254.169.254`` (the AWS/GCP/Azure metadata address),
``127.0.0.1:<admin-port>`` or an RFC-1918 host, the request pivots the
worker against its own internal network.

The gate has four layers, each of which has been the hole in a real
audit:

1. **Structural** — scheme allowlist (``file://`` reads the filesystem,
   ``gopher://`` injects into Redis), no userinfo, no fragment, port
   allowlist, no control characters, optional IP-literal refusal.
2. **Address classification** — :func:`is_non_public_address` treats
   anything not globally routable as unsafe, and unwraps the forms that
   defeat naive checks: IPv4-mapped IPv6 (``::ffff:127.0.0.1``), 6to4
   (``2002::/16``), and the historical IPv4 obfuscations libc still
   resolves (``2130706433``, ``0x7f000001``, ``0177.0.0.1``).
3. **Full DNS sweep** — every ``getaddrinfo`` answer must be public. A
   single-lookup check is defeated by DNS rebinding, where the resolver
   hands back a public address first and a private one next.
4. **Connect-time pinning** — :func:`resolve_outbound_url` returns the
   validated answers so the caller connects to a checked IP rather than
   re-resolving (the pre-flight/connect race).

Async callers must use the ``_async`` variants: ``getaddrinfo`` has no
timeout and inherits the system resolver's full retry schedule, so a
black-hole resolver would stall the event loop for tens of seconds.

Every failure path — parse error, DNS failure, malformed answer — is a
rejection. Default-deny is the point.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Callable, Iterable
from urllib.parse import urlsplit

from cara.security.UnsafeOutboundUrl import UnsafeOutboundUrl

#: Per-call DNS budget for the async variants. ``getaddrinfo`` has no
#: built-in timeout (glibc default: 5s × 2 attempts × N resolvers), and
#: 3s is ample for any real DNS hop.
DNS_RESOLUTION_TIMEOUT_SECONDS = 3

_Address = ipaddress.IPv4Address | ipaddress.IPv6Address

#: Hostnames that reach the loopback interface without ever touching a
#: resolver, so a structural (DNS-free) gate must reject them by name.
LOOPBACK_HOSTNAMES = frozenset(
    {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
)


def decode_obfuscated_ipv4(host: str) -> ipaddress.IPv4Address | None:
    """Decode the IPv4 forms libc resolves but ``ip_address`` rejects.

    Returns ``None`` when ``host`` is not number-shaped at all (i.e. it
    is a real DNS hostname). Handles the single decimal integer
    (``2130706433``), single hex (``0x7f000001``), dotted octets with
    octal/hex bases (``0177.0.0.1``) and the bare ``0``.
    """
    if not host or any(char in host for char in "/?#"):
        return None

    if "." not in host:
        try:
            packed = int(host, 0)
        except ValueError:
            return None
        if 0 <= packed <= 0xFFFFFFFF:
            try:
                return ipaddress.IPv4Address(packed)
            except (ValueError, ipaddress.AddressValueError):
                return None
        return None

    octets = host.split(".")
    if len(octets) != 4:
        return None
    packed = 0
    for octet in octets:
        if not octet:
            return None
        try:
            # ``int(x, 0)`` rejects a bare leading zero (``0177``), which is
            # exactly the octal form libc's inet_aton accepts — so the octal
            # base is applied explicitly rather than auto-detected.
            if octet.startswith(("0x", "0X", "0o", "0O")):
                value = int(octet, 0)
            elif len(octet) > 1 and octet.startswith("0"):
                value = int(octet, 8)
            else:
                value = int(octet)
        except ValueError:
            return None
        if not 0 <= value <= 255:
            return None
        packed = (packed << 8) | value
    try:
        return ipaddress.IPv4Address(packed)
    except (ValueError, ipaddress.AddressValueError):
        return None


def _unwrap(ip: _Address) -> _Address:
    """Follow IPv4-mapped and 6to4 IPv6 forms down to their IPv4."""
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            return ip.ipv4_mapped
        if ip.sixtofour is not None:
            return ip.sixtofour
    return ip


def is_non_public_address(addr: str | _Address) -> bool:
    """True unless ``addr`` is a globally routable unicast address.

    ``is_global`` already excludes loopback, RFC-1918, link-local
    (including the metadata address ``169.254.169.254``), unique-local
    ``fc00::/7``, CGNAT ``100.64.0.0/10``, reserved and unspecified
    ranges; multicast is refused explicitly because a multicast
    destination is never a legitimate outbound HTTP target. A value that
    does not parse as an address is unsafe — the caller resolved DNS, so
    a non-address here is a contract bug and must fail closed.
    """
    if isinstance(addr, str):
        raw = addr.split("%", 1)[0]  # strip an IPv6 zone id (fe80::1%eth0)
        try:
            ip: _Address = ipaddress.ip_address(raw)
        except ValueError:
            decoded = decode_obfuscated_ipv4(raw)
            if decoded is None:
                return True
            ip = decoded
    else:
        ip = addr
    ip = _unwrap(ip)
    return not ip.is_global or ip.is_multicast


def parse_host_allowlist(value: str | Iterable[str] | None) -> tuple[str, ...]:
    """Normalize exact/wildcard hostnames into a policy tuple.

    A ``*.example.com`` token permits subdomains but NOT the apex.
    Schemes, paths, ports, bare ``*`` and loopback-ish names are invalid
    entries and are dropped rather than broadening the policy. Returns
    ``()`` for an empty input — the caller decides whether an empty
    allowlist means "no restriction" or "deny everything".
    """
    if value is None:
        return ()
    candidates = (
        tuple(value.split(",")) if isinstance(value, str) else tuple(value or ())
    )

    normalized: list[str] = []
    for candidate in candidates:
        token = str(candidate or "").strip().lower().rstrip(".")
        wildcard = token.startswith("*.")
        base = token[2:] if wildcard else token
        if (
            not base
            or token == "*"
            or "://" in token
            or "/" in token
            or ":" in token
            or "*" in base
            or base in LOOPBACK_HOSTNAMES
            or base.endswith((".localhost", ".local", ".internal"))
        ):
            continue
        try:
            base = base.encode("idna").decode("ascii")
        except UnicodeError:
            continue
        normalized.append(f"*.{base}" if wildcard else base)
    return tuple(dict.fromkeys(normalized))


def host_matches_allowlist(host: str, allowlist: Iterable[str]) -> bool:
    """True when ``host`` matches an exact or wildcard policy entry."""
    return any(
        host.endswith(pattern[1:]) and host != pattern[2:]
        if pattern.startswith("*.")
        else host == pattern
        for pattern in allowlist
    )


def outbound_url_reason(
    url: object,
    *,
    allowed_schemes: Iterable[str] = ("https",),
    allowed_ports: Iterable[int] | None = None,
    allow_ip_literals: bool = False,
    allow_userinfo: bool = False,
    allow_fragment: bool = True,
    allow_non_public: bool = False,
    host_allowlist: Iterable[str] | None = None,
    resolver: Callable[..., list[tuple]] = socket.getaddrinfo,
    resolve_dns: bool = True,
    max_length: int = 2048,
) -> str | None:
    """Return ``None`` when the URL is safe, else a short reason.

    The reason strings are for logs and operator-facing messages;
    callers that need a typed failure use :func:`assert_outbound_url_safe`.
    ``allow_non_public`` is the local-development escape hatch — it skips
    the address checks only, never the structural ones.
    """
    raw = str(url or "").strip()
    if not raw:
        return "url is empty"
    if len(raw) > int(max_length):
        return "url is too long"
    if "\\" in raw or any(ord(char) < 0x20 or ord(char) == 0x7F for char in raw):
        return "url contains control characters"

    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        return f"url parse failed: {type(exc).__name__}"

    schemes = {str(scheme).lower() for scheme in allowed_schemes}
    if parsed.scheme.lower() not in schemes:
        return f"scheme not allowed: {parsed.scheme!r}"
    if not parsed.hostname:
        return "url missing hostname"
    if not allow_userinfo and (parsed.username is not None or parsed.password is not None):
        return "url must not contain userinfo"
    if not allow_fragment and parsed.fragment:
        return "url must not contain a fragment"
    if allowed_ports is not None and port is not None:
        if port not in {int(value) for value in allowed_ports}:
            return f"port not allowed: {port}"

    host = parsed.hostname.rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return "hostname is invalid"

    is_literal = True
    try:
        ipaddress.ip_address(host)
    except ValueError:
        is_literal = False
    if is_literal and not allow_ip_literals:
        return "url must use a hostname, not an IP literal"

    if host_allowlist is not None and not host_matches_allowlist(host, host_allowlist):
        return f"host not in allowlist: {host}"

    if allow_non_public:
        return None

    if host in LOOPBACK_HOSTNAMES:
        return f"loopback hostname: {host}"

    if is_literal:
        return None if not is_non_public_address(host) else f"non-public target: {host}"

    # A hostname that is really an obfuscated IPv4 literal never reaches
    # the resolver in some stacks; classify it before the DNS sweep.
    decoded = decode_obfuscated_ipv4(host)
    if decoded is not None:
        if not allow_ip_literals:
            return "url must use a hostname, not an IP literal"
        if is_non_public_address(decoded):
            return f"non-public target: {host}"
        return None

    if not resolve_dns:
        return None

    try:
        answers = resolver(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError) as exc:
        return f"dns resolution failed: {type(exc).__name__}"

    seen = False
    for answer in answers or []:
        sockaddr = answer[4] if len(answer) > 4 else None
        if not sockaddr:
            continue
        seen = True
        if is_non_public_address(str(sockaddr[0])):
            return f"hostname {host} resolves to non-public {sockaddr[0]}"
    if not seen:
        return f"hostname {host} does not resolve to a public address"
    return None


async def outbound_url_reason_async(
    url: object,
    *,
    dns_timeout: float = DNS_RESOLUTION_TIMEOUT_SECONDS,
    **kwargs,
) -> str | None:
    """:func:`outbound_url_reason` with a bounded DNS budget.

    ``getaddrinfo`` blocks; a hostile or black-hole resolver would hold
    the event loop for the system DNS timeout. Runs the gate in a worker
    thread and caps the wall clock, surfacing a rejection on timeout.
    """
    from cara.context import ExecutionContext  # local: cycle with cara.context

    def _check() -> str | None:
        return outbound_url_reason(url, **kwargs)

    try:
        return await asyncio.wait_for(
            ExecutionContext.run_in_thread(_check), timeout=float(dns_timeout)
        )
    except TimeoutError:
        return f"dns resolution timed out after {dns_timeout}s"


def assert_outbound_url_safe(url: object, *, label: str = "url", **kwargs) -> None:
    """Raise :class:`UnsafeOutboundUrl` when the URL fails the gate."""
    reason = outbound_url_reason(url, **kwargs)
    if reason is not None:
        raise UnsafeOutboundUrl(f"{label}: {reason}")


async def assert_outbound_url_safe_async(
    url: object, *, label: str = "url", **kwargs
) -> None:
    """Async :func:`assert_outbound_url_safe` with a bounded DNS budget."""
    reason = await outbound_url_reason_async(url, **kwargs)
    if reason is not None:
        raise UnsafeOutboundUrl(f"{label}: {reason}")


def resolve_outbound_url(
    url: object,
    *,
    label: str = "url",
    port: int = 443,
    resolver: Callable[..., list[tuple]] = socket.getaddrinfo,
    **kwargs,
) -> tuple[str, str, tuple[str, ...]]:
    """Validate and return ``(url, host, addresses)`` for connect-time pinning.

    Validating and then re-resolving at connect time is a race an
    attacker controls; the returned addresses are the ones that passed,
    and the caller connects to one of THOSE.
    """
    assert_outbound_url_safe(url, label=label, resolver=resolver, **kwargs)

    raw = str(url or "").strip()
    host = (urlsplit(raw).hostname or "").rstrip(".").lower()
    try:
        answers = resolver(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise UnsafeOutboundUrl(f"{label}: hostname could not be resolved") from exc

    addresses = {
        str(answer[4][0]).split("%", 1)[0]
        for answer in answers or []
        if len(answer) > 4 and answer[4]
    }
    if not addresses or any(is_non_public_address(address) for address in addresses):
        raise UnsafeOutboundUrl(f"{label}: hostname resolves to a non-public address")
    return raw, host, tuple(sorted(addresses))


__all__ = [
    "DNS_RESOLUTION_TIMEOUT_SECONDS",
    "LOOPBACK_HOSTNAMES",
    "assert_outbound_url_safe",
    "assert_outbound_url_safe_async",
    "decode_obfuscated_ipv4",
    "host_matches_allowlist",
    "is_non_public_address",
    "outbound_url_reason",
    "outbound_url_reason_async",
    "parse_host_allowlist",
    "resolve_outbound_url",
]
