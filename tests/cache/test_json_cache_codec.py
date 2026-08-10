from __future__ import annotations

import ast
import inspect
import pickle
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pendulum
import pytest

from cara.cache.codecs import JsonCacheCodec
from cara.exceptions import CacheConfigurationException

_KEY = b"cache-codec-test-key-material-32-bytes"
_PICKLE_EXECUTED = False


def _execute_pickle_gadget() -> None:
    global _PICKLE_EXECUTED
    _PICKLE_EXECUTED = True


class _PickleGadget:
    def __reduce__(self):
        return (_execute_pickle_gadget, ())


class _UnsupportedValue:
    pass


def test_round_trips_explicit_supported_types() -> None:
    codec = JsonCacheCodec(_KEY)
    value = {
        "none": None,
        "bool": True,
        "int": -(2**80),
        "float": -0.125,
        "str": "Cara 🛡️",
        "bytes": b"\x00\xff",
        "bytearray": bytearray(b"mutable"),
        "decimal": Decimal("1234.500"),
        "uuid": UUID("12345678-1234-5678-1234-567812345678"),
        "pendulum": pendulum.datetime(2026, 7, 16, 12, 30, tz="UTC"),
        "datetime": datetime(2026, 7, 16, 12, 30, tzinfo=UTC),
        "date": date(2026, 7, 16),
        "time": time(12, 30, 45, fold=1),
        "list": [1, "two"],
        "tuple": (1, "two"),
        "set": {"b", "a"},
        "frozenset": frozenset({1, 2}),
        ("tuple", "key"): {"nested": "value"},
    }

    decoded = codec.decode(codec.encode(value))

    assert decoded == value
    assert isinstance(decoded["bytearray"], bytearray)
    assert isinstance(decoded["pendulum"], pendulum.DateTime)
    assert decoded["time"].fold == 1


def test_encoding_is_canonical_for_dicts_and_sets() -> None:
    codec = JsonCacheCodec(_KEY)
    first = {"z": {3, 1, 2}, "a": {"b": 2, "a": 1}}
    second = {"a": {"a": 1, "b": 2}, "z": {2, 3, 1}}

    assert codec.encode(first) == codec.encode(second)


def test_tampered_payload_is_rejected() -> None:
    codec = JsonCacheCodec(_KEY)
    payload = bytearray(codec.encode({"role": "viewer"}))
    payload[-2] ^= 1

    with pytest.raises(CacheConfigurationException, match="integrity"):
        codec.decode(payload)


def test_unsigned_integer_and_legacy_pickle_are_never_decoded() -> None:
    global _PICKLE_EXECUTED
    _PICKLE_EXECUTED = False
    codec = JsonCacheCodec(_KEY)
    malicious_pickle = pickle.dumps(_PickleGadget())

    with pytest.raises(CacheConfigurationException, match="codec prefix"):
        codec.decode(b"1")
    with pytest.raises(CacheConfigurationException, match="codec prefix"):
        codec.decode(malicious_pickle)

    assert _PICKLE_EXECUTED is False


def test_custom_objects_and_non_finite_numbers_are_rejected() -> None:
    codec = JsonCacheCodec(_KEY)

    with pytest.raises(CacheConfigurationException, match="explicit scalar"):
        codec.encode(_UnsupportedValue())
    with pytest.raises(CacheConfigurationException, match="non-finite floats"):
        codec.encode(float("nan"))
    with pytest.raises(CacheConfigurationException, match="non-finite decimals"):
        codec.encode(Decimal("Infinity"))


def test_key_must_have_at_least_32_bytes() -> None:
    with pytest.raises(CacheConfigurationException, match="at least 32 bytes"):
        JsonCacheCodec(b"short")


def test_depth_budget_rejects_pathological_values() -> None:
    codec = JsonCacheCodec(_KEY)
    value: object = "leaf"
    for _ in range(codec.MAX_DEPTH + 1):
        value = [value]

    with pytest.raises(CacheConfigurationException, match="nested too deeply"):
        codec.encode(value)


def test_pendulum_is_a_hard_module_level_import() -> None:
    """pendulum is in cara's ``install_requires``; the codec must not pretend
    it is optional.

    Pre-fix ``_encode_value`` probed ``pendulum.DateTime`` behind a
    ``try: import pendulum`` / ``except ImportError, AttributeError`` whose
    fallback set ``is_pendulum = False``. That fallback is NOT inert:
    ``pendulum.DateTime`` subclasses ``datetime.datetime``, so the value fell
    through to the ``datetime`` branch and was written with tag ``"datetime"``.
    Redis then handed it back as a stdlib ``datetime`` — a silent type change
    no caller can see, in a codec whose docstring promises every supported
    type has an explicit tag. The decode side carried the same guard.
    """
    source = Path(inspect.getfile(JsonCacheCodec)).read_text(encoding="utf-8")
    tree = ast.parse(source)

    module_level_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "pendulum" in module_level_imports

    function_local_imports = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom) and node not in tree.body
    ]
    assert function_local_imports == []
