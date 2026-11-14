"""A FormRequest refuses a key its ``rules()`` do not declare (Stripe norm).

Dropping it answered 200 for a request that did not do what its sender meant:
``?status=unshipped`` listed every order and ``{"price": ...}`` on a door
without a price changed nothing.
"""

from __future__ import annotations

import asyncio
import importlib

import pytest

from cara.exceptions import ValidationException
from cara.http.requests.FormRequest import FormRequest


class _Request:
    def __init__(self, payload: dict, params: dict | None = None) -> None:
        self.payload = payload
        self.params = params or {}

    async def all(self):
        return dict(self.payload)


class _Orders(FormRequest):
    def rules(self):
        return {"limit": "nullable|integer", "items.*.sku": "sometimes|string"}

    def prepare_for_validation(self, data):
        if "per_page" in data:
            data["limit"] = data.pop("per_page")
        return data


_form_request = importlib.import_module("cara.http.requests.FormRequest")


@pytest.fixture(autouse=True)
def strict_product(monkeypatch):
    """A product that opted in (``server.refuse_unknown_request_keys``)."""
    settings = {"server.refuse_unknown_request_keys": True}
    monkeypatch.setattr(
        _form_request, "config", lambda key, default=None: settings.get(key, default)
    )
    return settings


def _run(payload: dict, params: dict | None = None):
    return asyncio.run(_Orders().validate_request(_Request(payload, params)))


def test_a_product_that_did_not_opt_in_keeps_ignoring_unknown_keys(strict_product):
    # Cheapa (2026-11-14 gate): flipped framework-wide, its unsubscribe link's
    # ``user`` and its lists' ``limit``/``offset`` became 422s.
    strict_product.clear()
    assert _run({"limit": 5, "status": "unshipped"}) == {"limit": 5}


def test_a_door_can_pin_the_policy_either_way(strict_product):
    class _Lenient(_Orders):
        refuse_unknown_keys = False

    assert asyncio.run(
        _Lenient().validate_request(_Request({"limit": 1, "status": "x"}))
    ) == {"limit": 1}
    strict_product.clear()

    class _Strict(_Orders):
        refuse_unknown_keys = True

    with pytest.raises(ValidationException):
        asyncio.run(_Strict().validate_request(_Request({"status": "x"})))


def test_an_undeclared_key_is_refused_with_its_name():
    with pytest.raises(ValidationException) as refused:
        _run({"limit": 5, "status": "unshipped"})
    assert "status" in refused.value.errors
    assert "status" in str(refused.value)


def test_declared_nested_and_aliased_keys_and_path_params_pass():
    validated = _run(
        {"items": [{"sku": "A"}], "per_page": 3, "id": "PRD1"}, {"id": "PRD1"}
    )
    assert validated["limit"] == 3
