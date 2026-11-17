"""``cara.support.opaque_reference`` — keyed, stable, non-enumerable references."""

from __future__ import annotations

import pytest

import cara.support.OpaqueReference as module
from cara.support import opaque_reference

_KEY = "k" * 40


@pytest.fixture(autouse=True)
def _app_key(monkeypatch):
    monkeypatch.setattr(
        module, "config", lambda key, default=None: _KEY if key == "app.key" else default
    )


def test_stable_and_namespaced():
    first = opaque_reference("price-alert", 7, domain="demo:opaque:v1")
    assert first == opaque_reference("price-alert", 7, domain="demo:opaque:v1")
    assert first.startswith("price-alert_")


def test_value_namespace_and_domain_each_change_the_reference():
    base = opaque_reference("wishlist", 1, domain="a:v1")
    assert base != opaque_reference("wishlist", 2, domain="a:v1")
    assert (
        base.split("_", 1)[1]
        != opaque_reference("saved", 1, domain="a:v1").split("_", 1)[1]
    )
    assert base != opaque_reference("wishlist", 1, domain="b:v1")


@pytest.mark.parametrize(
    "namespace", ["", "1abc", "UPPER_case", "x", "a" * 30, "has space"]
)
def test_bad_namespace_is_refused(namespace):
    with pytest.raises(ValueError):
        opaque_reference(namespace, 1, domain="a:v1")


def test_domain_is_required():
    with pytest.raises(ValueError):
        opaque_reference("wishlist", 1, domain=" ")


def test_short_key_is_refused(monkeypatch):
    monkeypatch.setattr(module, "config", lambda key, default=None: "short")
    with pytest.raises(RuntimeError):
        opaque_reference("wishlist", 1, domain="a:v1")
