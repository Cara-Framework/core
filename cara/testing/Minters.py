"""Id minters for test data: UNIQUE column values and model ``public_id``s.

Lane-agnostic — any test that needs a UNIQUE column value or a well-formed
``public_id`` uses them, database or no database.
"""

from __future__ import annotations

import itertools

import ulid

#: ``public_id`` columns are varchar(30): prefix + ULID(26), clamped.
PUBLIC_ID_LENGTH = 30

_seq = itertools.count(1)


def unique(prefix: str = "") -> str:
    """A process-unique token for populating UNIQUE columns.

    LOWERCASE on purpose. The columns this fills — email, slug, sku, url — are
    normalized to lower case on the way into production, and code downstream
    reads them that way (a sweep keyed on ``str(user.email).lower()``). A
    fixture holding a mixed-case ULID is a value no production path could
    store, and it walks straight past such a lowercasing read.
    """
    return f"{prefix}{next(_seq)}-{ulid.new()}".lower()


def public_id(model: type) -> str:
    """A bounded ULID ``public_id`` carrying ``model``'s own prefix.

    The prefix is READ from ``model.__public_id_prefix__`` — the attribute
    ``MakesPublicId`` mints production ids from — never restated in the test
    (DOCTRINE: read the SSOT, never restate it, fixtures included). A
    hand-copied literal keeps minting the old prefix after a model rename, and
    the lane goes on passing against ids no production code could produce.

    Use this for ``public_id`` columns, NOT :func:`unique`, whose counter grows
    past 30 characters once a full-suite run takes it multi-digit.
    """
    if not isinstance(model, type):
        # ``public_id("PP")`` is the restated literal this helper replaces.
        raise TypeError(f"public_id() takes a model class, got {model!r}")
    # The same read ``MakesPublicId`` makes: a model without a prefix mints a
    # bare ULID in production, so it does here.
    prefix = getattr(model, "__public_id_prefix__", "") or ""
    if not isinstance(prefix, str):
        raise TypeError(f"{model.__name__}.__public_id_prefix__ is not a string")
    return f"{prefix}{ulid.new()}"[:PUBLIC_ID_LENGTH]
