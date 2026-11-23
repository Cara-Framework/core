"""The ledger digest is taken from the canonical bytes, without a second parse.

``_delivery_register`` canonicalized the envelope and then passed the
canonical bytes to ``canonical_envelope_sha256``, which parsed and
re-serialized them to hash exactly what it had been given: one extra
``json.loads`` + ``json.dumps`` of the whole envelope per dispatch, inside
the dispatch transaction (audit, 2026-11-23).

``payload_sha256`` is an immutable ledger column the worker and the relay
re-derive and compare, so the shortcut is only admissible if it is
byte-identical to the old derivation for EVERY input. This file is that
proof: a differential against the old function over hand-picked and generated
envelopes, as bytes, as text and as dicts, including the inputs it refuses.
"""

from __future__ import annotations

import hashlib
import json
import random

import pytest

from cara.exceptions import QueueException
from cara.queues.delivery._DeliveryConfiguration import _canonical_envelope_and_digest
from cara.queues.serializers import SignedJsonJobSerializer as Serializer


def _old_digest(body) -> str:
    """The derivation registration used before: canonicalize, then hash by
    parsing and canonicalizing the canonical bytes again."""
    return Serializer.canonical_envelope_sha256(Serializer.canonical_envelope_bytes(body))


def _wire(payload_json: str, *, spaced: bool = False) -> str:
    """A signed-envelope-shaped JSON text around a raw ``payload`` fragment.

    Keys are deliberately NOT in canonical order and the spacing is not
    canonical either: the canonical form must come from the serializer.
    """
    gap = " \n\t" if spaced else ""
    return (
        "{" + gap + '"signature":"c2ln",' + gap + '"payload":' + payload_json + ","
        '"not_before":1763900000,"kid":"k1",' + gap + '"issued_at":1763900000,'
        '"expires_at": 1763990000' + gap + "}"
    )


_PAYLOAD_FRAGMENTS = [
    "{}",
    "[]",
    "null",
    '{"b":1,"a":2,"B":3,"A":4,"10":5,"2":6,"":7}',
    '{"job":{"class":"PushJob","module":"app.jobs","kwargs":{"task_id":7}}}',
    # Number spellings the canonical form rewrites.
    "[1E5, 1e-7, -0.0, 1.0, 0.1, 1e22, 5e-324, 1.7976931348623157e308, -1E+2]",
    "[0, -0, 123456789012345678901234567890, -9223372036854775808]",
    # String escapes: all of these have a different canonical spelling.
    r'"café 😀 \u0000 \/ \" \\ \n \t \u007f  "',
    '"Zażółć gęślą jaźń — 日本語 — 😀"',
    '{"é":"ü","\\u00e9x":"\\u00fc"}',
    "[[[[[[[[[[1]]]]]]]]]]",
    '{"a":{"b":{"c":{"d":[true,false,null,{"e":[]}]}}}}',
    '[ 1 , 2 ,\n 3 , { "k" : "v" } ]',
]


def _random_json(rng: random.Random, depth: int = 0):
    kinds = ["int", "float", "str", "bool", "none"]
    if depth < 4:
        kinds += ["list", "dict", "dict"]
    kind = rng.choice(kinds)
    if kind == "int":
        return rng.choice([0, -1, 7, 2**31, -(2**63), 10**30, rng.randint(-999, 999)])
    if kind == "float":
        return rng.choice(
            [0.0, -0.0, 1.5, 1e-9, 1e21, 1e22, 123456.789, rng.uniform(-1e6, 1e6)]
        )
    if kind == "str":
        alphabet = [
            "a",
            "Z",
            "0",
            " ",
            '"',
            "\\",
            "/",
            "\n",
            "\x00",
            "é",
            "ż",
            "日",
            "😀",
        ]
        return "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
    if kind == "bool":
        return rng.random() < 0.5
    if kind == "none":
        return None
    if kind == "list":
        return [_random_json(rng, depth + 1) for _ in range(rng.randint(0, 5))]
    keys = ["a", "b", "A", "10", "2", "é", "", "key with space", "z" * 20, "😀"]
    return {
        key: _random_json(rng, depth + 1) for key in rng.sample(keys, rng.randint(0, 6))
    }


def _generated_wires() -> list[str]:
    rng = random.Random(20261123)
    wires = []
    for _ in range(300):
        payload = _random_json(rng)
        fragment = json.dumps(
            payload,
            ensure_ascii=rng.random() < 0.5,
            sort_keys=rng.random() < 0.5,
            indent=rng.choice([None, None, 1, 4]),
        )
        wires.append(_wire(fragment, spaced=rng.random() < 0.5))
    return wires


_WIRES = [_wire(fragment) for fragment in _PAYLOAD_FRAGMENTS] + [
    _wire(fragment, spaced=True) for fragment in _PAYLOAD_FRAGMENTS
]


@pytest.mark.parametrize("wire", _WIRES)
def test_the_digest_matches_the_old_derivation_for_wire_text(wire):
    for body in (wire, wire.encode("utf-8"), json.loads(wire)):
        canonical, digest = _canonical_envelope_and_digest(body)

        assert canonical == Serializer.canonical_envelope_bytes(body)
        assert digest == _old_digest(body)
        # And it is what every later verifier derives from the stored bytes.
        assert digest == Serializer.canonical_envelope_sha256(canonical)


def test_the_digest_matches_the_old_derivation_over_generated_envelopes():
    for wire in _generated_wires():
        for body in (wire, wire.encode("utf-8"), json.loads(wire)):
            canonical, digest = _canonical_envelope_and_digest(body)
            assert digest == _old_digest(body), wire
            assert canonical == Serializer.canonical_envelope_bytes(body), wire


def test_wire_text_is_parsed_once(monkeypatch):
    parses: list[type] = []
    original = Serializer._parse_envelope.__func__

    def _counting(cls, body):
        parses.append(type(body))
        return original(cls, body)

    monkeypatch.setattr(Serializer, "_parse_envelope", classmethod(_counting))
    wire = _wire(_PAYLOAD_FRAGMENTS[4]).encode("utf-8")

    _canonical_envelope_and_digest(wire)
    assert parses == [bytes], "registration parsed the envelope a second time"

    parses.clear()
    _old_digest(wire)
    assert parses == [bytes, bytes]


def test_a_dict_with_non_string_keys_keeps_the_reparsed_digest():
    """Why a ``dict`` body is not hashed as it stands: ``json.dumps`` orders
    ``2`` before ``10`` and then writes them as ``"2"``, ``"10"``; the text
    read back orders ``"10"`` first. The ledger digest has always been the
    re-read form, which is also what a JSONB round trip hands the verifier."""
    body = json.loads(_wire("null"))
    body["payload"] = {"by_id": {2: "b", 10: "a"}, "pair": (1, 2)}

    canonical, digest = _canonical_envelope_and_digest(body)

    assert digest == _old_digest(body)
    assert digest != hashlib.sha256(canonical).hexdigest()
    assert digest == Serializer.canonical_envelope_sha256(json.loads(canonical))


@pytest.mark.parametrize(
    "refused",
    [
        _wire(r'"\ud800"'),  # a lone surrogate cannot be written as UTF-8
        _wire("NaN"),
        _wire("[1e999]"),
        _wire('{"a":1,"a":2}'),
        '{"payload":{}}',
        "[]",
        "not json",
        b"\xff\xfe",
    ],
    ids=["surrogate", "nan", "overflow", "duplicate", "shape", "array", "text", "utf8"],
)
def test_what_the_old_derivation_refused_is_still_refused(refused):
    with pytest.raises(QueueException) as old:
        _old_digest(refused)
    with pytest.raises(QueueException) as new:
        _canonical_envelope_and_digest(refused)
    assert str(new.value) == str(old.value)


def test_a_canonical_form_over_the_wire_limit_is_still_refused(monkeypatch):
    """``1E5`` is written back as ``100000.0``: a body inside the limit can
    canonicalize past it. The re-parse used to be what bounded that."""
    wire = _wire("[" + ",".join(["1E5"] * 200) + "]").encode("utf-8")
    monkeypatch.setattr(Serializer, "MAX_PAYLOAD_BYTES", len(wire))
    assert len(Serializer.canonical_envelope_bytes(wire)) > len(wire)

    with pytest.raises(QueueException) as old:
        _old_digest(wire)
    with pytest.raises(QueueException) as new:
        _canonical_envelope_and_digest(wire)
    assert str(new.value) == str(old.value)
