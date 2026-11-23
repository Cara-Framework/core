"""``QueuePayload.require_plain`` refuses non-plain data, naming the field.

Live defect: a queued mailable / notification / listener carried whatever
objects it held. The signed JSON envelope refuses anything but JSON
primitives by design, so the dispatch failed deep in the serializer with no
hint of WHICH attribute was the problem (or, before it, nothing queued at
all). ``require_plain`` now checks the data at dispatch and names the path
of the offending field, so the caller knows what to replace with an id.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cara.exceptions import InvalidArgumentException
from cara.queues import QueuePayload


def test_plain_data_passes_and_tuples_become_lists() -> None:
    value = {
        "id": 7,
        "ok": True,
        "ratio": 0.5,
        "name": "x",
        "tags": ("a", "b"),
        "none": None,
    }

    assert QueuePayload.require_plain(value, "data") == {
        "id": 7,
        "ok": True,
        "ratio": 0.5,
        "name": "x",
        "tags": ["a", "b"],
        "none": None,
    }


@pytest.mark.parametrize(
    ("value", "path"),
    [
        ({"user": object()}, r"mailable_data\.user"),
        (
            {"view_data": {"order": {"placed": datetime.now(UTC)}}},
            r"mailable_data\.view_data\.order\.placed",
        ),
        ({"items": [1, Decimal("2.5")]}, r"mailable_data\.items\[1\]"),
        ({"price": math.nan}, r"mailable_data\.price is a non-finite float"),
        ({"ids": {1, 2}}, r"mailable_data\.ids is a set"),
    ],
)
def test_non_plain_data_is_refused_naming_the_field(value, path) -> None:
    with pytest.raises(InvalidArgumentException, match=path):
        QueuePayload.require_plain(value, "mailable_data")


def test_a_non_string_key_is_refused() -> None:
    with pytest.raises(InvalidArgumentException, match="non-string key 3"):
        QueuePayload.require_plain({"by_id": {3: "x"}}, "data")


def test_runaway_nesting_is_refused() -> None:
    nested: list = []
    for _ in range(QueuePayload.MAX_DEPTH + 2):
        nested = [nested]

    with pytest.raises(InvalidArgumentException, match="nested too deeply"):
        QueuePayload.require_plain(nested, "data")
