"""Pin ``cara.support.install_numeric_coercion`` — the JSON-boundary rule
both products' job base classes install from ``__init_subclass__``."""

from __future__ import annotations

import inspect
from typing import Optional

import pytest

from cara.support import install_numeric_coercion
from cara.support.NumericCoercion import numeric_annotation_kind


class _Base:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        install_numeric_coercion(cls)


class _Probe(_Base):
    def __init__(
        self, price: float, qty: int, sku: str, ratio: float | None = None
    ) -> None:
        self.price, self.qty, self.sku, self.ratio = price, qty, sku, ratio


class _Child(_Probe):
    pass


def test_numeric_strings_follow_their_annotations() -> None:
    probe = _Probe(price="35.00", qty=" 7 ", sku="123", ratio="0.5")
    assert probe.price == 35.0 and isinstance(probe.price, float)
    assert probe.qty == 7 and isinstance(probe.qty, int)
    assert probe.ratio == 0.5
    assert probe.sku == "123"  # a str annotation is never coerced


def test_typed_values_none_and_garbage_pass_through() -> None:
    probe = _Probe(price=40.0, qty=True, sku="x", ratio="not-a-number")
    assert probe.price == 40.0 and probe.qty is True and probe.ratio == "not-a-number"
    assert _Probe(price="", qty="1", sku="y").price == ""
    assert _Probe(price=1.0, qty=1, sku="y", ratio=None).ratio is None


def test_argument_errors_come_from_the_real_constructor() -> None:
    with pytest.raises(TypeError, match="unexpected keyword"):
        _Probe(price="1", qty="1", sku="s", bogus=1)


def test_signature_stays_visible_and_install_is_idempotent() -> None:
    params = inspect.signature(_Probe.__dict__["__init__"]).parameters
    assert {"price", "qty", "sku", "ratio"} <= set(params)
    wrapped = _Probe.__dict__["__init__"]
    install_numeric_coercion(_Probe)
    assert _Probe.__dict__["__init__"] is wrapped
    assert "__init__" not in _Child.__dict__  # inherits, no second wrapper
    assert _Child(price="2", qty="3", sku="z").qty == 3


def test_class_without_numeric_annotations_is_left_alone() -> None:
    class _Plain(_Base):
        def __init__(self, name: str) -> None:
            self.name = name

    assert not hasattr(_Plain.__dict__["__init__"], "__wrapped__")


@pytest.mark.parametrize(
    ("annotation", "kind"),
    [
        ("int", "int"),
        ("int | None", "int"),
        ("Optional[int]", "int"),
        (Optional[int], "int"),  # noqa: UP045 - the live typing form is the case under test
        (int | None, "int"),
        (int, "int"),
        ("float | int", "float"),
        (float, "float"),
        ("str", None),
        ("list[int]", None),
        (inspect.Parameter.empty, None),
    ],
)
def test_numeric_annotation_kind(annotation, kind) -> None:
    assert numeric_annotation_kind(annotation) == kind
