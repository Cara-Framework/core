"""One trusted-proxy boundary, read from one config key.

Whether the immediate peer is a trusted reverse proxy decides three separate
things: whose ``X-Forwarded-For`` may dictate :meth:`Request.ip`, whose
``X-Forwarded-Proto`` may prove the connection was HTTPS, and therefore whether
``Strict-Transport-Security`` is attached at all. Those three answers must come
from ONE source, because a framework that trusts a peer for one and not the
others is not enforcing a boundary — it is enforcing two different boundaries
and calling them the same name.

It did exactly that. Three implementations had drifted apart inside the
framework:

* ``Request._trusted_proxy_networks`` read ``app.trusted_proxies``, auto-added
  loopback, and had no ``"*"`` sentinel.
* ``SecurityHeaders._peer_is_trusted_proxy`` read
  ``trustedproxies.proxies`` falling back to ``security.security.trusted_proxies``,
  auto-added nothing, and honoured ``"*"``.
* ``DefaultExceptionHandler`` carried a verbatim copy of the second.

**Neither config key the second and third read exists in any product.** No
product ships a ``config/trustedproxies.py``, and no product's ``SECURITY`` dict
carries ``trusted_proxies`` — while both products DO set ``TRUSTED_PROXIES``,
which lands on ``app.trusted_proxies``, the key only the first one read. So the
second and third resolved to ``[]`` on every request in every environment:
``_peer_is_trusted_proxy`` could never return ``True``, ``_is_https`` fell
through to the raw ASGI scheme, and behind a TLS-terminating proxy — the
Cloudflare-tunnel topology both products deploy — that scheme is ``http``.

The consequence was silent and total: ``Strict-Transport-Security`` was NEVER
emitted in production, despite HSTS being enabled by default, while the
framework simultaneously honoured the operator's configured proxies for
``request.ip()``. Nothing failed, no test covered the gap, and the header's
absence is invisible from inside the application.

Consolidating on ``app.trusted_proxies`` is what makes the boundary real: the
key an operator actually sets is the key every consumer reads.

Config accepts what operators actually write — a comma-separated string
(``TRUSTED_PROXIES=10.0.0.0/8,192.168.1.5``) or a list — and host bits are
tolerated (``10.0.0.5/8``) so pasting your own address with a netmask is not an
error. Invalid entries are skipped rather than raising: a typo in an allow-list
must not take the process down, and a warning is the right way to surface it.

**Loopback is auto-trusted; RFC1918 is not.** Spoofing the loopback peer
requires already being the process, so it adds no attack surface. Private
ranges are a different matter — auto-trusting them once let any compromised
sidecar, debug container or same-VPC pod spoof ``X-Forwarded-For`` and so
dictate ``request.ip()``, defeating per-IP rate limits, audit-log
non-repudiation and IP allow-lists. Default Docker (``172.17/16``) and k8s pod
(``10.244/16``) networks made that the out-of-the-box posture. Operators who
terminate TLS at an internal load balancer opt that range in explicitly, which
is what Symfony and Laravel 9+ also require.
"""

from __future__ import annotations

import contextlib
import ipaddress
from collections.abc import Iterable
from functools import lru_cache

from cara.configuration import config
from cara.exceptions import InvalidConfigurationSetupException
from cara.facades import Log

#: Auto-trusted regardless of configuration — see the module docstring.
LOOPBACK_NETWORKS: tuple[str, ...] = ("127.0.0.0/8", "::1/128")

#: Config value meaning "trust whatever peer is in front of us". Only correct
#: when nothing but a load balancer you control can reach the process.
TRUST_ALL_SENTINEL = "*"

_CONFIG_KEY = "app.trusted_proxies"


def parse_networks(
    raw: object,
) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """Coerce config into networks, skipping (and logging) invalid entries.

    Accepts a comma-separated string, an iterable of strings, or an iterable of
    comma-separated strings. Bare addresses become single-host networks.
    """
    if isinstance(raw, str):
        tokens: list[str] = [token.strip() for token in raw.split(",")]
    elif isinstance(raw, Iterable):
        tokens = []
        for entry in raw:
            if isinstance(entry, str):
                tokens.extend(token.strip() for token in entry.split(","))
    else:
        tokens = []

    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for token in tokens:
        if not token or token == TRUST_ALL_SENTINEL:
            continue
        try:
            # ``strict=False`` accepts ``10.0.0.5/8`` (host bits set) alongside
            # ``10.0.0.0/8``, so an operator pasting their own address with a
            # netmask is not rejected.
            networks.append(ipaddress.ip_network(token, strict=False))
        except ValueError:
            _warn_invalid(token)
    return tuple(networks)


def trusts_every_peer(raw: object) -> bool:
    """True when configuration carries the ``"*"`` trust-all sentinel."""
    if isinstance(raw, str):
        return TRUST_ALL_SENTINEL in (token.strip() for token in raw.split(","))
    if isinstance(raw, Iterable):
        return any(
            isinstance(entry, str)
            and TRUST_ALL_SENTINEL in (token.strip() for token in entry.split(","))
            for entry in raw
        )
    return False


def trusted_proxy_networks() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """Configured proxy networks, always including loopback."""
    return _networks_for(_raw_config())


def is_trusted_proxy(address: object) -> bool:
    """True when ``address`` is a configured trusted proxy.

    The single decision point for honouring any proxy-supplied header —
    ``X-Forwarded-For``, ``X-Forwarded-Proto`` and ``Forwarded`` alike.
    """
    raw = _raw_config()
    if trusts_every_peer(raw):
        return True
    if not isinstance(address, str) or not address:
        return False
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return any(ip in network for network in _networks_for(raw))


def peer_is_trusted_proxy(scope: object) -> bool:
    """``is_trusted_proxy`` for the peer of an ASGI ``scope``."""
    if not isinstance(scope, dict):
        return False
    client = scope.get("client") or ()
    return is_trusted_proxy(client[0] if client else None)


def _raw_config() -> object:
    """Read the config key late, so a test or a reload is never stale.

    Deliberately NOT cached: caching the *lookup* would freeze the boundary at
    whatever the first request saw. Only the parse below is memoized, keyed by
    the raw value, so repeated reads stay cheap while a config change still
    takes effect.
    """
    try:
        return config(_CONFIG_KEY, "") or ""
    except InvalidConfigurationSetupException:
        # Exception rendering can run while provider boot itself is failing.
        # The only safe pre-config policy is to trust no external peer; the
        # normal parser still includes process-local loopback.
        return ""


@lru_cache(maxsize=16)
def _parse_with_loopback(
    raw: str,
) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    return parse_networks(LOOPBACK_NETWORKS) + parse_networks(raw)


def _networks_for(
    raw: object,
) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    if isinstance(raw, str):
        return _parse_with_loopback(raw)
    # Unhashable config shapes (lists) cannot key the cache; parse directly.
    return parse_networks(LOOPBACK_NETWORKS) + parse_networks(raw)


def _warn_invalid(token: str) -> None:
    """Surface a bad allow-list entry without taking the process down."""
    with contextlib.suppress(Exception):
        Log.warning(
            f"TrustedProxies: ignoring invalid entry {token!r} in {_CONFIG_KEY}",
            category="security.trusted_proxies",
        )


__all__ = [
    "LOOPBACK_NETWORKS",
    "TRUST_ALL_SENTINEL",
    "is_trusted_proxy",
    "parse_networks",
    "peer_is_trusted_proxy",
    "trusted_proxy_networks",
    "trusts_every_peer",
]
