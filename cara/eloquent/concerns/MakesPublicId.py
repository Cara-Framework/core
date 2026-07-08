"""Mixin for models that generate prefixed public IDs (e.g., PROD_01J5K...).

Models declare their prefix via ``__public_id_prefix__`` class attribute.
"""

from __future__ import annotations

import ulid


class _AutoPublicIdObserver:
    """Model observer that fills ``public_id`` at create-time.

    WHY an observer and not a ``@creating``/``@saving`` decorator hook:
    ``Model.create()`` (the primary write path — and the path a new-record
    ``save()`` routes through) fires the *observer* ``creating``/``created``
    events from ``QueryBuilder.create``, but does NOT fire the decorator
    model-events (those only run inside ``Model.save``/``delete`` via
    ``_fire_model_event``). So a ``@creating`` method on this mixin would
    silently miss ``.create()`` — exactly the SalesOrder gap: every writer
    that didn't hand-pass ``generate_public_id()`` inserted a NULL/absent
    ``public_id``. Registering this observer on each ``MakesPublicId`` model
    (see ``boot_MakesPublicId``) makes auto-fill fire on the one choke point
    every writer actually uses.

    Stateless singleton — one instance is shared by every model. An explicit
    ``public_id`` supplied by the caller is always preserved.
    """

    def creating(self, model) -> None:
        prefix = getattr(type(model), "__public_id_prefix__", "")
        if not prefix:
            return
        already = model.__dirty_attributes__.get("public_id") or (
            model.__attributes__.get("public_id")
        )
        if not already:
            # Routes through Model.__setattr__ → __dirty_attributes__, which
            # QueryBuilder.create merges into the INSERT (self._creates) right
            # after firing 'creating'.
            model.public_id = model.generate_public_id()


_AUTO_PUBLIC_ID = _AutoPublicIdObserver()


class MakesPublicId:
    """Provides generate_public_id() from a declarative prefix, and
    auto-fills ``public_id`` on create for every model that mixes it in."""

    __public_id_prefix__: str = ""

    def boot_MakesPublicId(self, builder):
        """Required by ``Model.boot`` — and the auto-fill wiring point.

        ``Model.boot`` walks the MRO and, for every base class whose name
        starts with ``Makes``, unconditionally calls ``boot_<ClassName>`` (see
        ``cara/eloquent/models/Model.py``). The sibling mixins satisfy this
        contract — ``MakesSoftDeletes.boot_MakesSoftDeletes`` registers a
        global scope, etc. Without a matching ``boot_MakesPublicId`` here,
        booting ANY model that mixes this in raises
        ``AttributeError: class model 'X' has no attribute
        boot_MakesPublicId`` on the first query.

        We use this per-class boot hook to register :class:`_AutoPublicIdObserver`
        exactly once per concrete model, so ``public_id`` is stamped on every
        ``.create()`` without any per-caller ``generate_public_id()`` passing.
        The guard is keyed on ``cls.__dict__`` (not inherited) so each concrete
        subclass registers its own observer — ``observe_events`` dispatches by
        exact ``model.__class__``, and boot runs once per instance so a bare
        flag on the class is the idempotency key.
        """
        cls = type(self)
        if "_makes_public_id_observed" not in cls.__dict__:
            cls._makes_public_id_observed = True
            cls.observe(_AUTO_PUBLIC_ID)
        return None

    @classmethod
    def generate_public_id(cls) -> str:
        """Generate a prefixed ULID-based public ID."""
        return f"{cls.__public_id_prefix__}{str(ulid.new())}"
