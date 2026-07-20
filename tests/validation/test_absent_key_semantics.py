"""Absent keys are judged only by the presence-family rules (Laravel parity).

The defect this pins: an ABSENT key was fed to every rule in its chain as
``None``, so any non-nullable type rule rejected it. A bare
``"title": "string|max:512"`` — the idiomatic spelling for "optional, but a
real string when sent" — therefore made ``title`` *de facto required*. Over
the wire that meant no partial PATCH could reach a controller at all:

    PATCH /api/products/{id}  {"description": "x"}
    → 422 "The title field must be a string."   (title was never sent)

Laravel's rule (``presentOrRuleIsImplicit``): ``required`` is what makes
absence an error; ``nullable`` governs an explicitly-sent null, NOT absence.
Every other rule describes the shape of a value and has nothing to say about
a key that was never sent.

The wire half matters as much as the unit half — the suite stayed green
through this bug because every existing test drove the request/controller
objects directly, never ``validate_request``. These go through the seam the
route actually calls.
"""

from __future__ import annotations

import asyncio

import pytest

from cara.exceptions import ValidationException
from cara.http.requests import FormRequest
from cara.validation import Validation


class _FakeRequest:
    def __init__(self, data: dict) -> None:
        self._data = data

    async def all(self) -> dict:
        return dict(self._data)


class _PatchRequest(FormRequest):
    """The shape that was broken: optional-on-PATCH, null-invalid."""

    def rules(self) -> dict:
        return {
            "title": "string|max:512",
            "status": "string|in:active,archived",
            "description": "nullable|string",
        }


# --------------------------------------------------------------------- #
# Over the wire — through validate_request, the seam the route calls.    #
# --------------------------------------------------------------------- #


def test_partial_patch_body_passes_and_omits_absent_keys() -> None:
    validated = asyncio.run(
        _PatchRequest().validate_request(_FakeRequest({"description": "x"}))
    )
    # Absent optional keys stay OUT of validated() — reintroducing them as
    # synthetic ``None`` would turn every partial PATCH into an explicit
    # clear of every field the caller never mentioned.
    assert validated == {"description": "x"}


def test_empty_patch_body_is_not_an_error() -> None:
    assert asyncio.run(_PatchRequest().validate_request(_FakeRequest({}))) == {}


def test_explicit_null_is_still_rejected_over_the_wire() -> None:
    """``nullable`` is absent from the chain, so a sent null must fail —
    this is the half of the contract that must NOT regress."""
    with pytest.raises(ValidationException):
        asyncio.run(_PatchRequest().validate_request(_FakeRequest({"title": None})))


def test_sent_values_are_still_shape_checked_over_the_wire() -> None:
    for payload in ({"title": 123}, {"title": "z" * 513}, {"status": "nope"}):
        with pytest.raises(ValidationException):
            asyncio.run(_PatchRequest().validate_request(_FakeRequest(payload)))


def test_required_still_fires_over_the_wire() -> None:
    class _StoreRequest(FormRequest):
        def rules(self) -> dict:
            return {"title": "required|string|max:512"}

    with pytest.raises(ValidationException):
        asyncio.run(_StoreRequest().validate_request(_FakeRequest({})))


# --------------------------------------------------------------------- #
# Unit level — the validator's own absence semantics.                    #
# --------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "rule_string",
    [
        "string|max:512",
        "integer|min:1",
        "in:new,used,refurbished",
        "string|in:active,archived",
        "numeric|between:0,100",
        "array",
        "dict",
        "boolean",
        "email",
        "uuid",
    ],
)
def test_absent_key_skips_shape_rules(rule_string: str) -> None:
    validator = Validation.make({}, {"field": rule_string})
    assert validator.passes(), validator.all_errors()
    assert validator.validated() == {}


@pytest.mark.parametrize(
    ("rule_string", "fails_when_absent"),
    [
        # Presence-family rules still judge absence — that is their whole job.
        ("required", True),
        ("required|string", True),
        ("present", True),
        ("accepted", True),
        ("missing", False),  # absence is what it wants
        ("prohibited", False),  # ditto
        ("filled|string", False),  # constrains only a key that IS present
    ],
)
def test_implicit_rules_still_run_for_absent_keys(
    rule_string: str, fails_when_absent: bool
) -> None:
    assert Validation.make({}, {"field": rule_string}).fails() is fails_when_absent


def test_conditional_required_rules_still_fire_for_absent_keys() -> None:
    # The conditional presence family reads OTHER fields, so it must keep
    # running even though its own key is missing.
    assert Validation.make({"other": "1"}, {"f": "required_with:other|string"}).fails()
    assert Validation.make({}, {"f": "required_with:other|string"}).passes()
    assert Validation.make({"t": "yes"}, {"f": "required_if:t,yes|string"}).fails()
    assert Validation.make({"t": "no"}, {"f": "required_if:t,yes|string"}).passes()
    assert Validation.make({}, {"f": "required_without:other|string"}).fails()


def test_absence_skip_does_not_swallow_explicitly_sent_values() -> None:
    """The skip keys off ABSENCE, never off falsiness — a sent ``0`` /
    ``False`` / ``""`` is still a value and is still shape-checked."""
    assert Validation.make({"f": 0}, {"f": "integer|min:1"}).fails()
    assert Validation.make({"f": False}, {"f": "string"}).fails()
    assert Validation.make({"f": ""}, {"f": "in:active,archived"}).fails()
    assert Validation.make({"f": None}, {"f": "string"}).fails()


def test_nullable_and_sometimes_keep_their_distinct_meanings() -> None:
    # nullable: an explicitly-sent null is ACCEPTED and lands as None.
    nullable = Validation.make({"f": None}, {"f": "nullable|string"})
    assert nullable.passes()
    assert nullable.validated() == {"f": None}

    # sometimes: absence skips, but a sent null still runs the chain and fails.
    assert Validation.make({}, {"f": "sometimes|string"}).passes()
    assert Validation.make({"f": None}, {"f": "sometimes|string"}).fails()

    # bare: absence skips (this fix) and a sent null fails — which is exactly
    # what ``sometimes`` gives, so neither modifier is needed to express
    # "optional on PATCH, but never explicitly null".
    assert Validation.make({}, {"f": "string"}).passes()
    assert Validation.make({"f": None}, {"f": "string"}).fails()
