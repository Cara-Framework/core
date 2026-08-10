"""Behaviour pins for the boot-time error-envelope validator.

The rule it enforces: every subclass of an application's exception base must
be able to render the canonical ``{status_code, to_dict()}`` envelope, so no
typed error silently degrades into the handler's generic body. The subtleties
that matter are which classes are EXEMPT (the base itself) and which ones
still count (inherited envelopes).
"""

from __future__ import annotations

import pytest

from cara.exceptions import validate_exception_envelopes


def _base():
    """A fresh base per test — ``__subclasses__`` is global and permanent."""

    class Base(Exception):
        status_code = 500

    return Base


def test_a_base_with_no_subclasses_passes():
    validate_exception_envelopes(_base())


def test_the_base_itself_is_exempt():
    """A base typically carries the fallback status and deliberately has no
    ``to_dict`` — it lands on the handler's generic path on purpose."""
    base = _base()
    assert not hasattr(base, "to_dict")

    validate_exception_envelopes(base)


def test_a_complete_hierarchy_passes():
    base = _base()

    class NotFound(base):
        status_code = 404

        def to_dict(self):
            return {"type": "not_found"}

    validate_exception_envelopes(base)


def test_a_subclass_without_to_dict_fails_boot():
    base = _base()

    class Silent(base):
        status_code = 409

    with pytest.raises(RuntimeError) as excinfo:
        validate_exception_envelopes(base)

    message = str(excinfo.value)
    assert "Silent" in message
    assert "no to_dict envelope" in message


def test_a_subclass_without_a_status_code_fails_boot():
    base = _base()

    class Untyped(base):
        status_code = None

        def to_dict(self):
            return {"type": "untyped"}

    with pytest.raises(RuntimeError) as excinfo:
        validate_exception_envelopes(base)

    assert "missing/invalid status_code" in str(excinfo.value)


def test_a_non_integer_status_code_fails_boot():
    """``"404"`` renders as a status line by accident and breaks by surprise."""
    base = _base()

    class Stringly(base):
        status_code = "404"

        def to_dict(self):
            return {"type": "stringly"}

    with pytest.raises(RuntimeError):
        validate_exception_envelopes(base)


def test_an_inherited_envelope_counts():
    """A subclass folded onto a parent that already renders an envelope reuses
    it — that is what the hierarchy is for."""
    base = _base()

    class Validation(base):
        status_code = 422

        def to_dict(self):
            return {"type": "validation_failed"}

    class BulkUpdateFailed(Validation):
        pass

    validate_exception_envelopes(base)


def test_the_walk_reaches_transitive_subclasses():
    base = _base()

    class Middle(base):
        status_code = 400

        def to_dict(self):
            return {"type": "middle"}

    class Leaf(Middle):
        status_code = None

    with pytest.raises(RuntimeError) as excinfo:
        validate_exception_envelopes(base)

    assert "Leaf" in str(excinfo.value)


def test_every_offender_is_named_and_sorted():
    base = _base()

    class Zulu(base):
        status_code = 400

    class Alpha(base):
        status_code = 400

    with pytest.raises(RuntimeError) as excinfo:
        validate_exception_envelopes(base)

    message = str(excinfo.value)
    assert message.index("Alpha") < message.index("Zulu")
    assert base.__name__ in message
