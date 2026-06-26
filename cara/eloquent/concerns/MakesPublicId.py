"""Mixin for models that generate prefixed public IDs (e.g., PROD_01J5K...).

Models declare their prefix via ``__public_id_prefix__`` class attribute.
"""

from __future__ import annotations

import ulid


class MakesPublicId:
    """Provides generate_public_id() from a declarative prefix."""

    __public_id_prefix__: str = ""

    def boot_MakesPublicId(self, builder):
        """Required by ``Model.boot``.

        ``Model.boot`` walks the MRO and, for every base class whose name
        starts with ``Makes``, unconditionally calls ``boot_<ClassName>`` (see
        ``cara/eloquent/models/Model.py``). The sibling mixins satisfy this
        contract — ``MakesSoftDeletes.boot_MakesSoftDeletes`` registers a
        global scope, etc. Without a matching ``boot_MakesPublicId`` here,
        booting ANY model that mixes this in raises
        ``AttributeError: class model 'X' has
        no attribute boot_MakesPublicId`` on the first query — which
        surfaced as 500s across every model-eager-loading endpoint
        (e.g. ``/api/wishlist``).

        Public-id assignment is a save-time concern handled per-model
        (``@saving``/``@creating`` hooks and explicit ``generate_public_id``
        calls in ``create(...)``), so there is no query scope to register —
        this boot hook is intentionally a no-op that exists only to honour
        the framework's mixin-boot contract.
        """
        return None

    @classmethod
    def generate_public_id(cls) -> str:
        """Generate a prefixed ULID-based public ID."""
        return f"{cls.__public_id_prefix__}{str(ulid.new())}"
