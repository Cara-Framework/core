"""Security primitives."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DNS_RESOLUTION_TIMEOUT_SECONDS": (
        ".OutboundUrl",
        "DNS_RESOLUTION_TIMEOUT_SECONDS",
    ),
    "LOOPBACK_HOSTNAMES": (".OutboundUrl", "LOOPBACK_HOSTNAMES"),
    "LOOPBACK_NETWORKS": (".TrustedProxies", "LOOPBACK_NETWORKS"),
    "PinnedHTTPSConnection": (".PinnedHTTPSConnection", "PinnedHTTPSConnection"),
    "SignedToken": (".SignedToken", "SignedToken"),
    "TRUST_ALL_SENTINEL": (".TrustedProxies", "TRUST_ALL_SENTINEL"),
    "UnsafeOutboundUrl": (".UnsafeOutboundUrl", "UnsafeOutboundUrl"),
    "assert_outbound_url_safe": (".OutboundUrl", "assert_outbound_url_safe"),
    "assert_outbound_url_safe_async": (
        ".OutboundUrl",
        "assert_outbound_url_safe_async",
    ),
    "decode_obfuscated_ipv4": (".OutboundUrl", "decode_obfuscated_ipv4"),
    "download_public_https": (".PinnedHTTPSConnection", "download_public_https"),
    "host_matches_allowlist": (".OutboundUrl", "host_matches_allowlist"),
    "is_non_public_address": (".OutboundUrl", "is_non_public_address"),
    "is_trusted_proxy": (".TrustedProxies", "is_trusted_proxy"),
    "open_pinned_https": (".PinnedHTTPSConnection", "open_pinned_https"),
    "outbound_url_reason": (".OutboundUrl", "outbound_url_reason"),
    "outbound_url_reason_async": (".OutboundUrl", "outbound_url_reason_async"),
    "parse_host_allowlist": (".OutboundUrl", "parse_host_allowlist"),
    "parse_networks": (".TrustedProxies", "parse_networks"),
    "peer_is_trusted_proxy": (".TrustedProxies", "peer_is_trusted_proxy"),
    "require_independent_signing_key": (
        ".SigningKeys",
        "require_independent_signing_key",
    ),
    "require_signing_keyring": (".SigningKeys", "require_signing_keyring"),
    "resolve_outbound_url": (".OutboundUrl", "resolve_outbound_url"),
    "trusted_proxy_networks": (".TrustedProxies", "trusted_proxy_networks"),
    "trusts_every_peer": (".TrustedProxies", "trusts_every_peer"),
}

__all__ = [
    "DNS_RESOLUTION_TIMEOUT_SECONDS",
    "LOOPBACK_HOSTNAMES",
    "LOOPBACK_NETWORKS",
    "PinnedHTTPSConnection",
    "SignedToken",
    "TRUST_ALL_SENTINEL",
    "UnsafeOutboundUrl",
    "assert_outbound_url_safe",
    "assert_outbound_url_safe_async",
    "decode_obfuscated_ipv4",
    "download_public_https",
    "host_matches_allowlist",
    "is_non_public_address",
    "is_trusted_proxy",
    "open_pinned_https",
    "outbound_url_reason",
    "outbound_url_reason_async",
    "parse_host_allowlist",
    "parse_networks",
    "peer_is_trusted_proxy",
    "require_independent_signing_key",
    "require_signing_keyring",
    "resolve_outbound_url",
    "trusted_proxy_networks",
    "trusts_every_peer",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
