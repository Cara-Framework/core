"""Encoding a dict no longer costs one ``json.dumps`` per key.

Dict entries are ordered by their key's canonical JSON. That was computed
with ``json.dumps`` (a new encoder per call) for every key — more than half
of the whole encode on a page of product cards (451 KiB: 23 ms -> 13 ms).
A string key now has its canonical form written directly. The ORDER, and so
every stored byte, must not move: locks are released by comparing bytes.
"""

from __future__ import annotations

import json
from decimal import Decimal

from cara.cache.codecs import JsonCacheCodec

_KEY = b"codec-encode-cost-test-key-32-bytes!"


def _reference_entry_order(codec: JsonCacheCodec, mapping: dict) -> list:
    """The order the codec has always written: by canonical JSON bytes."""

    def canonical(key) -> bytes:
        return json.dumps(
            codec._encode_value(key, depth=1, budget=[0]),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    return sorted(mapping, key=canonical)


def test_string_keys_are_ordered_without_a_json_dump_each(monkeypatch) -> None:
    codec = JsonCacheCodec(_KEY)
    dumps = 0
    canonical_json = JsonCacheCodec._canonical_json

    def counting(value):
        nonlocal dumps
        dumps += 1
        return canonical_json(value)

    monkeypatch.setattr(JsonCacheCodec, "_canonical_json", staticmethod(counting))

    codec.encode([{f"field_{index}": index for index in range(50)} for _ in range(20)])

    # The envelope itself. It was 1 + 20 * 50 = 1,001.
    assert dumps == 1


def test_awkward_string_keys_keep_their_canonical_order() -> None:
    codec = JsonCacheCodec(_KEY)
    mapping = {
        key: index
        for index, key in enumerate(
            [
                "a",
                "a b",  # a space sorts BEFORE the closing quote of "a"
                'a"b',
                "a\\b",
                "a\nb",
                "a\x00b",
                "a}",
                "",
                "é",
                "z",
                " ",
                "퟿",
                "\U0001f600",
                "A",
                "~",
            ]
        )
    }

    decoded = codec.decode(codec.encode(mapping))

    assert list(decoded) == _reference_entry_order(codec, mapping)
    assert decoded == mapping


def test_mixed_key_types_keep_their_canonical_order() -> None:
    codec = JsonCacheCodec(_KEY)
    mapping = {
        "b": 1,
        7: 2,
        "a": 3,
        (1, "x"): 4,
        None: 5,
        True: 6,
        Decimal("1.5"): 7,
        -3: 8,
        "int": 9,
    }

    decoded = codec.decode(codec.encode(mapping))

    assert list(decoded) == _reference_entry_order(codec, mapping)
    assert decoded == mapping


def test_the_encoding_of_a_known_value_is_pinned() -> None:
    # Bytes written by the release before this change. A lock token or any
    # value compared with ``forget_if`` during a rolling deploy must encode
    # to the same bytes on both sides.
    codec = JsonCacheCodec(_KEY)

    payload = codec.encode({"b": [1, "x"], "a": {"z": None, "y": True}})[
        len(codec.MAGIC) + codec.TAG_BYTES :
    ]

    assert payload == (
        b'{"value":{"t":"dict","v":[[{"t":"str","v":"a"},{"t":"dict","v":'
        b'[[{"t":"str","v":"y"},{"t":"bool","v":true}],[{"t":"str","v":"z"},'
        b'{"t":"none"}]]}],[{"t":"str","v":"b"},{"t":"list","v":[{"t":"int",'
        b'"v":"1"},{"t":"str","v":"x"}]}]]},"version":1}'
    )


def test_a_string_subclass_key_still_encodes() -> None:
    class Label(str):
        pass

    codec = JsonCacheCodec(_KEY)

    assert codec.decode(codec.encode({Label("b"): 1, "a": 2})) == {"a": 2, "b": 1}
