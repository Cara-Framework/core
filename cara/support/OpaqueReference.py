"""Stable, non-reversible public references for rows without a public id.

Product-blind: the product passes its own ``domain`` (a separation string such
as ``"myapp:opaque-reference:v1"``) so two products sharing a key never mint
the same reference, and the HMAC root is ``app.key``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
from typing import Any

from cara.configuration import config

_NAMESPACE = re.compile(r"^[a-z][a-z0-9-]{1,23}$")
_MIN_KEY_BYTES = 32


def opaque_reference(namespace: str, value: Any, *, domain: str) -> str:
    """Return ``<namespace>_<token>`` — stable, keyed, not enumerable.

    A keyed digest gives a row a stable identity for clients without making
    its database id enumerable or reversible. ``namespace`` scopes the
    reference (``price-alert`` and ``wishlist`` ids never collide);
    ``domain`` separates products and versions.
    """
    scope = str(namespace or "").strip().lower()
    if not _NAMESPACE.fullmatch(scope):
        raise ValueError("opaque reference namespace is invalid")
    if not str(domain or "").strip():
        raise ValueError("opaque reference domain is required")
    root = str(config("app.key", "") or "").encode("utf-8")
    if len(root) < _MIN_KEY_BYTES:
        raise RuntimeError("APP_KEY must contain at least 32 bytes")
    label = f"{domain}:{scope}".encode()
    purpose_key = hmac.new(root, label, hashlib.sha256).digest()
    digest = hmac.new(purpose_key, str(value).encode(), hashlib.sha256).digest()[:18]
    token = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return f"{scope}_{token}"


__all__ = ["opaque_reference"]
