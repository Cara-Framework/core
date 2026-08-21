"""Scanning a model for event hooks must not touch its descriptors.

``_get_model_events`` walks every ``cls.__dict__`` in a model's MRO. A model
class body holds far more than methods — relationship descriptors
(``@belongs_to`` and friends) live there too, and
``BaseRelationship.__getattr__`` resolves the relationship by INSTANTIATING
the related model: ``Model.__init__`` -> ``boot()`` ->
``get_connection_details()`` -> the ``DB`` facade.

The old probe was ``hasattr(value, "_is_model_event")``. On a relationship
descriptor that misses natively and falls through to ``__getattr__``, so
merely ASKING a model which hooks it declares booted the application and hit
the database — and raised outright where no application is bootstrapped.

The marker is written into the decorated function's own ``__dict__``, so it
is now read from there. ``vars()`` resolves ``__dict__`` through the type and
can never reach ``__getattr__``.

The traps below stand in for the descriptor: one explodes when resolved (the
bootless failure), one resolves silently but records the touch (the booted
failure, where the damage is the boot, not an exception). Both pin the same
rule — discovery reads, it does not resolve.
"""

from __future__ import annotations

from cara.decorators.Events import _get_model_events, creating, deleting, saving


class _ExplodingDescriptor:
    """A descriptor whose attribute resolution fails, recording the touch.

    Mirrors ``BaseRelationship.__getattr__`` reaching an unbootstrapped
    application: the probe does not merely miss, it raises.
    """

    def __getattr__(self, attribute: str) -> object:
        """Record the probe, then refuse it the way a bootless resolve does."""
        self.__dict__.setdefault("touched", []).append(attribute)
        raise RuntimeError(f"relationship resolved during scan: {attribute}")


class _SilentDescriptor:
    """A descriptor that answers probes with ``AttributeError``, recording them.

    ``hasattr`` swallows ``AttributeError``, so the old probe looked clean
    here — yet the related model had already been built. This trap keeps the
    side effect visible after the exception is gone.
    """

    def __getattr__(self, attribute: str) -> object:
        """Record the probe, then miss the way a booted resolve would."""
        self.__dict__.setdefault("touched", []).append(attribute)
        raise AttributeError(attribute)


def _touched(descriptor: object) -> list[str]:
    """Attribute names a trap descriptor was probed for, read without probing."""
    return vars(descriptor).get("touched", [])


class _Base:
    """Model-like ancestor contributing one hook of its own."""

    @saving
    def normalize_on_base(self) -> None:
        """Inherited hook — proves the MRO walk still reaches base classes."""


class _ModelLike(_Base):
    """Model-like class: real hooks alongside descriptors that must stay untouched.

    Deliberately not a ``cara.Model`` subclass — instantiating one is the very
    thing this test forbids the scanner from causing.
    """

    brand = _ExplodingDescriptor()
    vendor = _SilentDescriptor()

    plain_attribute = "not a listener"

    @creating
    def assign_defaults(self) -> None:
        """First ``creating`` listener, alphabetically."""

    @creating
    def zz_last_listener(self) -> None:
        """Second ``creating`` listener, alphabetically."""

    @deleting
    def guard_delete(self) -> bool:
        """A cancelling listener, to keep more than one event in the mapping."""
        return False

    @property
    def computed(self) -> str:
        """A ``property`` has no instance dict of its own — must be skipped."""
        return "computed"


def test_discovery_finds_every_hook_on_a_model_carrying_descriptors() -> None:
    """The mapping's shape and contents are unchanged by the descriptor fix."""
    events = _get_model_events(_ModelLike)

    assert set(events) == {"creating", "saving", "deleting"}
    assert [listener.__name__ for listener in events["creating"]] == [
        "assign_defaults",
        "zz_last_listener",
    ]
    assert [listener.__name__ for listener in events["saving"]] == ["normalize_on_base"]
    assert [listener.__name__ for listener in events["deleting"]] == ["guard_delete"]


def test_discovery_never_resolves_a_relationship_descriptor() -> None:
    """The scan must not probe attributes on a descriptor it walks past.

    Against the old ``hasattr`` probe this test does not merely fail — it
    errors out of ``_get_model_events`` on ``_ExplodingDescriptor``, exactly
    as the kernel suite did before the fix.
    """
    _get_model_events(_ModelLike)

    assert _touched(_ModelLike.__dict__["brand"]) == []
    assert _touched(_ModelLike.__dict__["vendor"]) == []


def test_a_silent_descriptor_is_not_probed_either() -> None:
    """An ``AttributeError``-answering descriptor hid the boot from ``hasattr``.

    Scanned on its own, with no exploding sibling to abort the walk first:
    under the old probe this returns the very same correct mapping while
    having already resolved the relationship. So this is the half that pins
    the SIDE EFFECT rather than the crash.
    """

    class _SilentOnly:
        vendor = _SilentDescriptor()

        @saving
        def normalize(self) -> None:
            """The hook discovery must still find."""

    events = _get_model_events(_SilentOnly)

    assert [listener.__name__ for listener in events["saving"]] == ["normalize"]
    assert _touched(_SilentOnly.__dict__["vendor"]) == []


def test_a_model_without_hooks_scans_to_an_empty_mapping() -> None:
    """Discovery returns a plain empty dict, not None, when nothing is marked."""

    class _NoHooks:
        brand = _ExplodingDescriptor()

    assert _get_model_events(_NoHooks) == {}
    assert _touched(_NoHooks.__dict__["brand"]) == []
