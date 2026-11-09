"""IPv6 forms that carry an internal IPv4 are not public addresses.

``is_non_public_address`` unwraps IPv4-mapped and 6to4 addresses and leaves the
rest to ``is_global``, which answers True for the well-known NAT64 prefix and
for the deprecated IPv4-compatible range. A hostname the seller controls can
publish these as AAAA records, and the DNS sweep then waves them through.
"""

from __future__ import annotations

import socket

import pytest

from cara.security.OutboundUrl import is_non_public_address, outbound_url_reason

_EMBEDDED_INTERNAL = [
    "64:ff9b::a9fe:a9fe",  # NAT64 well-known prefix, 169.254.169.254
    "64:ff9b::7f00:1",  # NAT64 well-known prefix, 127.0.0.1
    "64:ff9b::a00:1",  # NAT64 well-known prefix, 10.0.0.1
    "::a9fe:a9fe",  # IPv4-compatible, 169.254.169.254
    "::127.0.0.1",  # IPv4-compatible, loopback
    "::ffff:0:a9fe:a9fe",  # IPv4-translated (SIIT), 169.254.169.254
]


@pytest.mark.parametrize("address", _EMBEDDED_INTERNAL)
def test_an_internal_ipv4_inside_ipv6_is_not_public(address):
    assert is_non_public_address(address) is True


@pytest.mark.parametrize("address", _EMBEDDED_INTERNAL)
def test_the_dns_sweep_refuses_an_aaaa_record_that_embeds_one(address):
    def resolver(_host, _port, **_kwargs):
        return [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", (address, 443, 0, 0))]

    assert outbound_url_reason("https://rebind.example/x", resolver=resolver) is not None


def test_a_nat64_address_of_a_public_ipv4_stays_public():
    assert is_non_public_address("64:ff9b::808:808") is False
