"""``FormRequest.prepare_for_validation`` — Laravel's prepareForValidation hook.

Pins the contract that motivated it: a subclass that normalizes the RAW payload
in the hook has its mutation RE-VALIDATED by ``rules()`` (the bug of normalizing
by overriding ``validate_request`` post-hoc was that the mutation skipped
validation), and the default hook is a transparent passthrough.
"""

from __future__ import annotations

import asyncio

from cara.http.requests import FormRequest


class _FakeRequest:
    def __init__(self, data: dict) -> None:
        self._data = data

    async def all(self) -> dict:
        return dict(self._data)


class _AliasRequest(FormRequest):
    """Aliases ``per_page`` → ``limit`` BEFORE validation so the rule sees it."""

    def rules(self) -> dict:
        return {"limit": "required|integer"}

    def prepare_for_validation(self, data: dict) -> dict:
        if "per_page" in data and "limit" not in data:
            data["limit"] = data.pop("per_page")
        return data


class _PassthroughRequest(FormRequest):
    def rules(self) -> dict:
        return {"name": "required|string"}


def test_prepare_for_validation_mutation_is_revalidated() -> None:
    # The hook injects ``limit`` from ``per_page``; it must pass the ``integer``
    # rule and surface in ``validated()`` — proving the mutation was validated.
    validated = asyncio.run(
        _AliasRequest().validate_request(_FakeRequest({"per_page": 25}))
    )
    assert validated["limit"] == 25


def test_prepare_for_validation_default_is_passthrough() -> None:
    validated = asyncio.run(
        _PassthroughRequest().validate_request(_FakeRequest({"name": "x"}))
    )
    assert validated == {"name": "x"}
