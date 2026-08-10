"""The outbound-URL gate — the union of both products' rejection matrices.

Each case here was a real hole in a real audit, not a hypothetical: the
cloud metadata address, DNS rebinding through a mixed answer set, the
IPv4-mapped-IPv6 bypass, libc's obfuscated IPv4 forms, scheme-based SSRF,
and the black-hole resolver that hangs an event loop.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket

import pytest

from cara.security import (
    UnsafeOutboundUrl,
    assert_outbound_url_safe,
    decode_obfuscated_ipv4,
    host_matches_allowlist,
    is_non_public_address,
    outbound_url_reason,
    outbound_url_reason_async,
    parse_host_allowlist,
    resolve_outbound_url,
)


def _resolver(*addresses: str):
    """Fake getaddrinfo returning the given addresses."""

    def _resolve(_host, _port=None, **_kw):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (addr, 443)) for addr in addresses
        ]

    return _resolve


def _public():
    return _resolver("93.184.216.34")


# ── address classification ───────────────────────────────────────────


@pytest.mark.parametrize(
    "address",
    [
        "169.254.169.254",  # AWS/GCP/Azure metadata — the classic pivot
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "100.64.0.1",  # CGNAT: shared address space, often internal infra
        "0.0.0.0",
        "224.0.0.1",  # multicast
        "192.0.2.1",  # TEST-NET-1
        "::1",
        "fe80::1",
        "fc00::1",  # unique-local
        "::ffff:127.0.0.1",  # IPv4-mapped IPv6 — the naive-check bypass
        "2002:7f00:1::",  # 6to4 wrapping 127.0.0.1
    ],
)
def test_non_public_addresses_are_rejected(address: str) -> None:
    assert is_non_public_address(address) is True


@pytest.mark.parametrize("address", ["93.184.216.34", "8.8.8.8", "2606:4700::1111"])
def test_public_addresses_pass(address: str) -> None:
    assert is_non_public_address(address) is False


def test_zone_id_is_stripped_before_classification() -> None:
    assert is_non_public_address("fe80::1%eth0") is True


def test_an_unparseable_address_fails_closed() -> None:
    assert is_non_public_address("not-an-address") is True


def test_accepts_already_parsed_addresses() -> None:
    assert is_non_public_address(ipaddress.ip_address("127.0.0.1")) is True
    assert is_non_public_address(ipaddress.ip_address("93.184.216.34")) is False


# ── libc's obfuscated IPv4 forms ─────────────────────────────────────


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("2130706433", "127.0.0.1"),  # decimal
        ("0x7f000001", "127.0.0.1"),  # hex
        ("0177.0.0.1", "127.0.0.1"),  # octal first octet
        ("0", "0.0.0.0"),
    ],
)
def test_obfuscated_ipv4_forms_decode(host: str, expected: str) -> None:
    assert decode_obfuscated_ipv4(host) == ipaddress.IPv4Address(expected)


def test_a_real_hostname_is_not_an_obfuscated_address() -> None:
    assert decode_obfuscated_ipv4("example.com") is None
    assert decode_obfuscated_ipv4("api.internal-service") is None


def test_obfuscated_loopback_is_refused_as_an_ip_literal() -> None:
    reason = outbound_url_reason("https://2130706433/x", resolver=_public())
    assert reason is not None and "IP literal" in reason


# ── structural gate ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "gopher://127.0.0.1:6379/_SET%20k%20v",
        "data:text/plain,hi",
        "http://example.com/",  # http when only https is allowed
    ],
)
def test_scheme_allowlist(url: str) -> None:
    reason = outbound_url_reason(url, resolver=_public())
    assert reason is not None and "scheme not allowed" in reason


def test_userinfo_is_refused() -> None:
    reason = outbound_url_reason("https://user:pw@example.com/x", resolver=_public())
    assert reason is not None and "userinfo" in reason


def test_fragment_can_be_refused() -> None:
    assert outbound_url_reason("https://example.com/x#f", resolver=_public()) is None
    reason = outbound_url_reason(
        "https://example.com/x#f", allow_fragment=False, resolver=_public()
    )
    assert reason is not None and "fragment" in reason


def test_port_allowlist() -> None:
    reason = outbound_url_reason(
        "https://example.com:8443/x", allowed_ports=(443,), resolver=_public()
    )
    assert reason is not None and "port not allowed" in reason
    assert (
        outbound_url_reason(
            "https://example.com:443/x", allowed_ports=(443,), resolver=_public()
        )
        is None
    )


def test_control_characters_are_refused() -> None:
    reason = outbound_url_reason("https://example.com/\r\nX-Evil: 1", resolver=_public())
    assert reason is not None and "control characters" in reason


def test_ip_literals_are_refused_by_default_and_can_be_allowed() -> None:
    reason = outbound_url_reason("https://93.184.216.34/x", resolver=_public())
    assert reason is not None and "IP literal" in reason
    assert (
        outbound_url_reason(
            "https://93.184.216.34/x", allow_ip_literals=True, resolver=_public()
        )
        is None
    )


def test_a_public_ip_literal_passes_but_a_private_one_never_does() -> None:
    reason = outbound_url_reason(
        "http://169.254.169.254/latest/meta-data/",
        allowed_schemes=("http", "https"),
        allow_ip_literals=True,
        resolver=_public(),
    )
    assert reason is not None and "non-public target" in reason


def test_loopback_hostnames_are_refused_without_dns() -> None:
    def _explode(*_a, **_kw):
        raise AssertionError("must not reach the resolver")

    reason = outbound_url_reason("https://localhost/x", resolver=_explode)
    assert reason is not None and "loopback hostname" in reason


def test_an_empty_url_is_refused() -> None:
    assert outbound_url_reason("") is not None
    assert outbound_url_reason(None) is not None


# ── DNS sweep ────────────────────────────────────────────────────────


def test_a_public_hostname_passes() -> None:
    assert outbound_url_reason("https://example.com/x", resolver=_public()) is None


def test_a_mixed_answer_set_fails_closed() -> None:
    """DNS rebinding: public first, private second. One bad answer is enough."""
    reason = outbound_url_reason(
        "https://example.com/x", resolver=_resolver("93.184.216.34", "127.0.0.1")
    )
    assert reason is not None and "non-public" in reason


def test_an_empty_answer_set_is_a_rejection() -> None:
    reason = outbound_url_reason("https://example.com/x", resolver=lambda *a, **kw: [])
    assert reason is not None and "does not resolve" in reason


def test_a_failing_resolver_is_a_rejection() -> None:
    def _fail(*_a, **_kw):
        raise socket.gaierror("nope")

    reason = outbound_url_reason("https://example.com/x", resolver=_fail)
    assert reason is not None and "dns resolution failed" in reason


def test_dns_can_be_skipped_for_structure_only_callers() -> None:
    def _explode(*_a, **_kw):
        raise AssertionError("must not resolve")

    assert (
        outbound_url_reason("https://example.com/x", resolve_dns=False, resolver=_explode)
        is None
    )


def test_allow_non_public_is_a_dev_hatch_that_keeps_structural_checks() -> None:
    assert (
        outbound_url_reason(
            "http://127.0.0.1:8000/x",
            allowed_schemes=("http",),
            allow_ip_literals=True,
            allow_non_public=True,
        )
        is None
    )
    # Structure is still enforced under the hatch.
    reason = outbound_url_reason(
        "file:///etc/passwd", allow_non_public=True, allowed_schemes=("http",)
    )
    assert reason is not None and "scheme" in reason


# ── host allowlist ───────────────────────────────────────────────────


def test_allowlist_parsing_drops_invalid_entries() -> None:
    parsed = parse_host_allowlist(
        "fcm.googleapis.com, *.push.example.com, *, https://x.com, host:443, localhost"
    )
    assert parsed == ("fcm.googleapis.com", "*.push.example.com")


def test_wildcard_matches_subdomains_but_not_the_apex() -> None:
    policy = parse_host_allowlist("*.push.example.com")
    assert host_matches_allowlist("a.push.example.com", policy) is True
    assert host_matches_allowlist("push.example.com", policy) is False


def test_a_host_outside_the_allowlist_is_refused() -> None:
    reason = outbound_url_reason(
        "https://evil.example/x",
        host_allowlist=parse_host_allowlist("fcm.googleapis.com"),
        resolver=_public(),
    )
    assert reason is not None and "allowlist" in reason


# ── typed failure + pinning ──────────────────────────────────────────


def test_assert_raises_the_typed_error_with_its_label() -> None:
    with pytest.raises(UnsafeOutboundUrl) as excinfo:
        assert_outbound_url_safe(
            "https://example.com/x", label="webhook url", resolver=_resolver("10.0.0.1")
        )
    assert "webhook url" in str(excinfo.value)


def test_unsafe_outbound_url_is_a_value_error() -> None:
    """Callers that already catch ValueError keep working."""
    assert issubclass(UnsafeOutboundUrl, ValueError)


def test_resolve_returns_the_checked_addresses_for_pinning() -> None:
    url, host, addresses = resolve_outbound_url(
        "https://example.com/x",
        resolver=_resolver("93.184.216.34", "93.184.216.35"),
        allowed_ports=(443,),
    )
    assert url == "https://example.com/x"
    assert host == "example.com"
    assert addresses == ("93.184.216.34", "93.184.216.35")


def test_resolve_refuses_a_non_public_answer() -> None:
    with pytest.raises(UnsafeOutboundUrl):
        resolve_outbound_url("https://example.com/x", resolver=_resolver("127.0.0.1"))


# ── async budget ─────────────────────────────────────────────────────


def test_async_gate_agrees_with_the_sync_one() -> None:
    reason = asyncio.run(
        outbound_url_reason_async("https://example.com/x", resolver=_public())
    )
    assert reason is None

    reason = asyncio.run(
        outbound_url_reason_async("https://example.com/x", resolver=_resolver("10.0.0.1"))
    )
    assert reason is not None and "non-public" in reason


def test_a_black_hole_resolver_cannot_stall_the_event_loop() -> None:
    """The AWAIT must return on budget even though the thread runs on.

    ``getaddrinfo`` is not cancellable, so the worker thread keeps
    blocking until the OS resolver gives up — what the budget protects is
    the event loop, which must be free to serve everything else. Timing
    is therefore measured around the await, not around loop shutdown.
    """
    import time

    def _hang(*_a, **_kw):
        time.sleep(1.5)
        return []

    async def _drive() -> tuple[str | None, float]:
        started = time.monotonic()
        reason = await outbound_url_reason_async(
            "https://example.com/x", resolver=_hang, dns_timeout=0.2
        )
        return reason, time.monotonic() - started

    reason, elapsed = asyncio.run(_drive())
    assert reason is not None and "timed out" in reason
    assert elapsed < 1.0, "the DNS budget must bound the await"
