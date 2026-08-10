"""Typed rejection for an outbound URL that failed the SSRF gate."""

from __future__ import annotations

from cara.exceptions.types.Base import CaraException


class UnsafeOutboundUrl(CaraException, ValueError):
    """An outbound destination is not safe to open a socket to.

    A ``ValueError`` subclass so existing callers that catch ``ValueError``
    around URL policy keep working, while new code can catch the precise
    class. Products subclass it when their surface needs its own name
    (an unsafe push endpoint, an unsafe fetch target).

    Also a ``CaraException``, because §9 has ONE taxonomy and an SSRF
    rejection outside it is invisible to ``except CaraException`` — the
    clause a worker wraps around an outbound hop when it wants to fail the
    job cleanly rather than let an unexpected fault escape. ``types.Base``
    is imported directly: the security gate is reachable from very early
    boot paths and the ``cara.exceptions`` barrel drags in the foundation.
    """


__all__ = ["UnsafeOutboundUrl"]
