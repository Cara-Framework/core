"""Hardened client-IP resolution for audit and security records.

Client IPs are routinely read straight off ``X-Forwarded-For`` /
``X-Real-Ip``. Any unauthenticated client can set those headers to whatever
it likes, so a record written from them is a record of what the *client
claimed* — worthless for non-repudiation. :meth:`Request.ip` already
implements the TRUSTED_PROXIES walk: forwarded-for headers are honoured only
when the immediate peer is a trusted proxy, otherwise it falls back to the
ASGI client tuple.

:func:`trusted_client_ip` is that value, made safe to call from the paths
that record it. It is deliberately duck-typed and never raises: audit and
security writes must not be the thing that fails a request, and they are
routinely exercised with stand-in request objects (fakes, slotted mocks,
``SimpleNamespace``) that may have no ``ip`` at all. A missing or failing
``ip()`` yields ``None`` — an absent IP — never a forged one and never an
exception.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger("cara.support.ClientIp")


def trusted_client_ip(request: Any) -> str | None:
    """Return the trusted client IP for ``request``, or ``None``.

    ``None`` means "not determinable here" — record it as an absent IP. It
    never falls back to a client-supplied header, because a forged IP in an
    audit trail is worse than no IP at all.
    """
    ip_fn = getattr(request, "ip", None)
    if not callable(ip_fn):
        return None
    try:
        return ip_fn()
    except Exception:
        _logger.warning("trusted_client_ip: request.ip() failed", exc_info=True)
        return None


__all__ = ["trusted_client_ip"]
