"""
HasAttributes Concern

Single Responsibility: Handle all attribute-related operations for Eloquent models.
Extracted from Model.py to follow SRP and DRY principles.
"""

from __future__ import annotations

from typing import Any

from cara.eloquent.casts import cast_registry


class HasAttributes:
    """
    Mixin for handling model attributes and cast-on-write.

    ``Model`` — this concern's only consumer — owns the attribute STORES
    (``__attributes__`` / ``__original_attributes__`` /
    ``__dirty_attributes__``) and rebinds mass assignment, serialization,
    visibility and dirty tracking to the ``_Model*`` implementations. Every
    member this concern used to carry for those jobs was shadowed by those
    bindings, so the bodies here never ran; the shadowed copies additionally
    addressed a retired parallel ``_attributes`` store and would have
    returned empty data had anything reached them.

    What is left is what ``Model`` does NOT rebind: the two public attribute
    accessors and the cast-on-write helper.
    """

    # ===== Attribute Access =====
    #
    # ``__getattr__``, ``__setattr__``, ``get_raw_attribute`` and
    # ``fill_original`` used to live here too. All four were shadowed by
    # ``Model`` — the concern's ONLY consumer — so their bodies never ran, and
    # all four addressed the retired parallel store. Keeping shadowed copies
    # around is not harmless: this concern's ``__setattr__`` delegated to
    # ``set_attribute``, so once ``set_attribute`` became the thin wrapper over
    # the real write door the pair formed an infinite recursion for any class
    # that mixed the concern in without ``Model``'s ``__setattr__``. Dead code
    # cannot be trusted to stay dead.

    def get_attribute(self, attribute: str) -> Any:
        """Get an attribute value with casting."""
        value = self.get_raw_attribute(attribute)

        if value is not None:
            return self._cast_attribute(attribute, value)

        return value

    def set_attribute(self, attribute: str, value: Any) -> None:
        """Set an attribute value — the same write ``model.attr = value`` performs.

        ``Model.__setattr__`` is the single write door: it applies ``@mutator``
        methods, the cast registry and date conversion, then records the value
        in ``__dirty_attributes__`` so ``save()`` can see it.

        Pre-fix this method kept its OWN parallel store — ``_attributes`` /
        ``_original`` / ``_changes`` — which ``Model`` never reads. The write
        silently went nowhere: ``m.set_attribute("foo", 5)`` left ``m.foo``
        missing, ``m.get_attribute("foo")`` answering ``None`` and ``save()``
        with nothing to persist. A public ORM setter that loses data is worse
        than one that raises, because nothing anywhere reports a failure.
        """
        setattr(self, attribute, value)

    # ===== Casting Support =====

    def _set_cast_attribute(self, attribute: str, value: Any) -> Any:
        """Cast value for setting attribute."""
        if attribute in self.__casts__:
            cast_type = self.__casts__[attribute]
            # Get cast instance and use set method

            cast_instance = cast_registry.get_cast_instance(cast_type)
            if cast_instance:
                return cast_instance.set(value)

        return value
