"""Post-validation hooks are part of the request's validation authority."""

from __future__ import annotations

import pytest

from cara.validation import Validation


def test_after_callback_failure_is_not_reported_as_a_valid_request() -> None:
    validator = Validation.make({"name": "valid"}, {"name": "required|string"})

    def broken_invariant(_validator: Validation) -> None:
        raise RuntimeError("invariant unavailable")

    validator.after(broken_invariant)

    with pytest.raises(RuntimeError, match="invariant unavailable"):
        validator.passes()
