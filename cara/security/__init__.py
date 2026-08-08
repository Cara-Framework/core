"""Security primitives."""

from .OutboundUrl import (
    DNS_RESOLUTION_TIMEOUT_SECONDS,
    LOOPBACK_HOSTNAMES,
    assert_outbound_url_safe,
    assert_outbound_url_safe_async,
    decode_obfuscated_ipv4,
    host_matches_allowlist,
    is_non_public_address,
    outbound_url_reason,
    outbound_url_reason_async,
    parse_host_allowlist,
    resolve_outbound_url,
)
from .PinnedHttps import PinnedHTTPSConnection, download_public_https, open_pinned_https
from .SigningKeys import require_independent_signing_key, require_signing_keyring
from .UnsafeOutboundUrl import UnsafeOutboundUrl

__all__ = [
    "DNS_RESOLUTION_TIMEOUT_SECONDS",
    "LOOPBACK_HOSTNAMES",
    "PinnedHTTPSConnection",
    "UnsafeOutboundUrl",
    "assert_outbound_url_safe",
    "assert_outbound_url_safe_async",
    "decode_obfuscated_ipv4",
    "download_public_https",
    "host_matches_allowlist",
    "is_non_public_address",
    "open_pinned_https",
    "outbound_url_reason",
    "outbound_url_reason_async",
    "parse_host_allowlist",
    "require_independent_signing_key",
    "require_signing_keyring",
    "resolve_outbound_url",
]
