"""Typed rejection for an outbound URL that failed the SSRF gate."""

from __future__ import annotations


class UnsafeOutboundUrl(ValueError):
    """An outbound destination is not safe to open a socket to.

    A ``ValueError`` subclass so existing callers that catch ``ValueError``
    around URL policy keep working, while new code can catch the precise
    class. Products subclass it when their surface needs its own name
    (an unsafe push endpoint, an unsafe fetch target).
    """


__all__ = ["UnsafeOutboundUrl"]
