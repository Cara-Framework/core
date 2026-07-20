"""Authenticated, non-executable Redis cache value codec."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from cara.exceptions import CacheConfigurationException


class JsonCacheCodec:
    """Canonical tagged JSON with HMAC integrity.

    Every supported Python type has an explicit tag. Decoding never imports a
    class, invokes a constructor selected by the payload, or executes object
    hooks. Redis ``INCRBY`` counters live in a separate key namespace and never
    pass through this value codec.
    """

    VERSION = 1
    NAMESPACE = "j1"
    MAGIC = b"CARA-CACHE-J1\x00"
    TAG_BYTES = 32
    MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
    MAX_DEPTH = 64
    MAX_NODES = 100_000
    _DOMAIN = b"cara.cache.redis.json.v1\x00"
    _RAW_INTEGER = re.compile(rb"-?(?:0|[1-9][0-9]*)\Z")

    def __init__(
        self,
        signing_key: str | bytes,
        *,
        max_nodes: int | None = None,
        max_payload_bytes: int | None = None,
    ):
        raw_key = (
            signing_key.encode("utf-8") if isinstance(signing_key, str) else signing_key
        )
        if not isinstance(raw_key, bytes) or len(raw_key) < 32:
            raise CacheConfigurationException(
                "Redis cache signing key must contain at least 32 bytes."
            )
        self._key = hashlib.sha256(self._DOMAIN + raw_key).digest()
        # Per-instance overrides of the structural safety caps. The class
        # defaults stay conservative; a trusted first-party cache with
        # legitimately large values (e.g. a catalog aggregate of enriched
        # product cards, or a deep category tree) can raise the node budget
        # via config. ``MAX_PAYLOAD_BYTES`` remains the hard byte bound, so
        # a raised node budget can never admit an oversized payload.
        if max_nodes is not None:
            self.MAX_NODES = max_nodes
        if max_payload_bytes is not None:
            self.MAX_PAYLOAD_BYTES = max_payload_bytes

    def encode(self, value: Any) -> bytes:
        budget = [0]
        tagged = self._encode_value(value, depth=0, budget=budget)
        payload = self._canonical_json({"version": self.VERSION, "value": tagged})
        if len(payload) > self.MAX_PAYLOAD_BYTES:
            raise CacheConfigurationException(
                f"Redis cache payload exceeds {self.MAX_PAYLOAD_BYTES} bytes."
            )
        tag = hmac.new(self._key, self._DOMAIN + payload, hashlib.sha256).digest()
        return self.MAGIC + tag + payload

    def decode(self, blob: bytes | bytearray | memoryview) -> Any:
        try:
            raw = bytes(blob)
        except (TypeError, ValueError) as exc:
            raise CacheConfigurationException(
                "Redis cache payload must be bytes."
            ) from exc

        minimum = len(self.MAGIC) + self.TAG_BYTES + 2
        if len(raw) < minimum or not raw.startswith(self.MAGIC):
            raise CacheConfigurationException(
                "Redis cache payload has an unsupported codec prefix."
            )
        if len(raw) > len(self.MAGIC) + self.TAG_BYTES + self.MAX_PAYLOAD_BYTES:
            raise CacheConfigurationException("Redis cache payload is too large.")

        tag_start = len(self.MAGIC)
        tag = raw[tag_start : tag_start + self.TAG_BYTES]
        payload = raw[tag_start + self.TAG_BYTES :]
        expected = hmac.new(
            self._key,
            self._DOMAIN + payload,
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(tag, expected):
            raise CacheConfigurationException(
                "Redis cache payload integrity verification failed."
            )

        try:
            envelope = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=self._reject_duplicate_json_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise CacheConfigurationException(
                "Redis cache payload is not valid UTF-8 JSON."
            ) from exc
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"version", "value"}
            or type(envelope.get("version")) is not int
            or envelope["version"] != self.VERSION
        ):
            raise CacheConfigurationException(
                "Redis cache payload has an unsupported envelope."
            )

        return self._decode_value(envelope["value"], depth=0, budget=[0])

    def _encode_value(self, value: Any, *, depth: int, budget: list[int]) -> Any:
        self._check_budget(depth, budget)
        if value is None:
            return {"t": "none"}
        if isinstance(value, bool):
            return {"t": "bool", "v": value}
        if isinstance(value, int):
            return {"t": "int", "v": str(value)}
        if isinstance(value, float):
            if not math.isfinite(value):
                raise CacheConfigurationException(
                    "Redis cache cannot encode non-finite floats."
                )
            return {"t": "float", "v": repr(value)}
        if isinstance(value, str):
            return {"t": "str", "v": value}
        if isinstance(value, bytes):
            return {
                "t": "bytes",
                "v": base64.b64encode(value).decode("ascii"),
            }
        if isinstance(value, bytearray):
            return {
                "t": "bytearray",
                "v": base64.b64encode(bytes(value)).decode("ascii"),
            }
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise CacheConfigurationException(
                    "Redis cache cannot encode non-finite decimals."
                )
            return {"t": "decimal", "v": str(value)}
        if isinstance(value, UUID):
            return {"t": "uuid", "v": str(value)}

        try:
            import pendulum

            is_pendulum = isinstance(value, pendulum.DateTime)
        except (ImportError, AttributeError):
            is_pendulum = False
        if is_pendulum:
            return {"t": "pendulum", "v": value.to_iso8601_string()}
        if isinstance(value, datetime):
            return {
                "t": "datetime",
                "v": value.isoformat(),
                "fold": int(value.fold),
            }
        if isinstance(value, date):
            return {"t": "date", "v": value.isoformat()}
        if isinstance(value, time):
            return {
                "t": "time",
                "v": value.isoformat(),
                "fold": int(value.fold),
            }
        if isinstance(value, list):
            return {
                "t": "list",
                "v": [
                    self._encode_value(item, depth=depth + 1, budget=budget)
                    for item in value
                ],
            }
        if isinstance(value, tuple):
            return {
                "t": "tuple",
                "v": [
                    self._encode_value(item, depth=depth + 1, budget=budget)
                    for item in value
                ],
            }
        if isinstance(value, (set, frozenset)):
            encoded = [
                self._encode_value(item, depth=depth + 1, budget=budget) for item in value
            ]
            encoded.sort(key=self._canonical_json)
            return {
                "t": "frozenset" if isinstance(value, frozenset) else "set",
                "v": encoded,
            }
        if isinstance(value, dict):
            entries = []
            for key, item in value.items():
                encoded_key = self._encode_value(
                    key,
                    depth=depth + 1,
                    budget=budget,
                )
                encoded_value = self._encode_value(
                    item,
                    depth=depth + 1,
                    budget=budget,
                )
                entries.append(
                    (self._canonical_json(encoded_key), [encoded_key, encoded_value])
                )
            entries.sort(key=lambda entry: entry[0])
            return {"t": "dict", "v": [entry[1] for entry in entries]}

        raise CacheConfigurationException(
            f"Redis cache cannot encode {type(value).__module__}."
            f"{type(value).__name__}; cache DTOs must use explicit scalar/"
            "container types."
        )

    def _decode_value(self, node: Any, *, depth: int, budget: list[int]) -> Any:
        self._check_budget(depth, budget)
        if not isinstance(node, dict) or "t" not in node:
            raise CacheConfigurationException("Redis cache value tag is invalid.")
        tag = node.get("t")

        if tag == "none":
            self._require_keys(node, {"t"})
            return None
        self._require_keys(
            node,
            {"t", "v", "fold"} if tag in {"datetime", "time"} else {"t", "v"},
        )
        value = node["v"]

        if tag == "bool" and isinstance(value, bool):
            return value
        if tag == "int" and isinstance(value, str):
            try:
                encoded_integer = value.encode("ascii")
            except UnicodeEncodeError:
                encoded_integer = b""
            if self._RAW_INTEGER.fullmatch(encoded_integer):
                return int(value)
        if tag == "float" and isinstance(value, str):
            try:
                decoded = float(value)
            except ValueError as exc:
                raise CacheConfigurationException(
                    "Redis cache float tag is invalid."
                ) from exc
            if math.isfinite(decoded):
                return decoded
        if tag == "str" and isinstance(value, str):
            return value
        if tag in {"bytes", "bytearray"} and isinstance(value, str):
            try:
                decoded_bytes = base64.b64decode(value, validate=True)
            except (ValueError, TypeError) as exc:
                raise CacheConfigurationException(
                    "Redis cache byte tag is invalid."
                ) from exc
            return decoded_bytes if tag == "bytes" else bytearray(decoded_bytes)
        if tag == "decimal" and isinstance(value, str):
            try:
                decoded_decimal = Decimal(value)
            except InvalidOperation as exc:
                raise CacheConfigurationException(
                    "Redis cache decimal tag is invalid."
                ) from exc
            if decoded_decimal.is_finite():
                return decoded_decimal
        if tag == "uuid" and isinstance(value, str):
            try:
                return UUID(value)
            except ValueError as exc:
                raise CacheConfigurationException(
                    "Redis cache UUID tag is invalid."
                ) from exc
        if tag == "pendulum" and isinstance(value, str):
            try:
                import pendulum

                return pendulum.parse(value)
            except (ImportError, ValueError, TypeError) as exc:
                raise CacheConfigurationException(
                    "Redis cache pendulum tag is invalid."
                ) from exc
        if tag == "datetime" and isinstance(value, str):
            try:
                return datetime.fromisoformat(value).replace(fold=self._fold(node))
            except ValueError as exc:
                raise CacheConfigurationException(
                    "Redis cache datetime tag is invalid."
                ) from exc
        if tag == "date" and isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError as exc:
                raise CacheConfigurationException(
                    "Redis cache date tag is invalid."
                ) from exc
        if tag == "time" and isinstance(value, str):
            try:
                return time.fromisoformat(value).replace(fold=self._fold(node))
            except ValueError as exc:
                raise CacheConfigurationException(
                    "Redis cache time tag is invalid."
                ) from exc
        if tag in {"list", "tuple", "set", "frozenset"} and isinstance(value, list):
            decoded_items = [
                self._decode_value(item, depth=depth + 1, budget=budget) for item in value
            ]
            if tag == "list":
                return decoded_items
            if tag == "tuple":
                return tuple(decoded_items)
            try:
                return (
                    frozenset(decoded_items) if tag == "frozenset" else set(decoded_items)
                )
            except TypeError as exc:
                raise CacheConfigurationException(
                    "Redis cache set contains an unhashable value."
                ) from exc
        if tag == "dict" and isinstance(value, list):
            decoded_dict = {}
            for entry in value:
                if not isinstance(entry, list) or len(entry) != 2:
                    raise CacheConfigurationException(
                        "Redis cache dict entry is invalid."
                    )
                key = self._decode_value(
                    entry[0],
                    depth=depth + 1,
                    budget=budget,
                )
                item = self._decode_value(
                    entry[1],
                    depth=depth + 1,
                    budget=budget,
                )
                try:
                    if key in decoded_dict:
                        raise CacheConfigurationException(
                            "Redis cache dict contains a duplicate key."
                        )
                    decoded_dict[key] = item
                except TypeError as exc:
                    raise CacheConfigurationException(
                        "Redis cache dict contains an unhashable key."
                    ) from exc
            return decoded_dict

        raise CacheConfigurationException(
            f"Redis cache value tag {tag!r} is invalid or unsupported."
        )

    @staticmethod
    def _canonical_json(value: Any) -> bytes:
        try:
            return json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise CacheConfigurationException(
                f"Redis cache value is not canonical JSON: {exc}"
            ) from exc

    @staticmethod
    def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    def _check_budget(self, depth: int, budget: list[int]) -> None:
        if depth > self.MAX_DEPTH:
            raise CacheConfigurationException("Redis cache value is nested too deeply.")
        budget[0] += 1
        if budget[0] > self.MAX_NODES:
            raise CacheConfigurationException(
                "Redis cache value contains too many elements."
            )

    @staticmethod
    def _require_keys(node: dict[str, Any], expected: set[str]) -> None:
        if set(node) != expected:
            raise CacheConfigurationException("Redis cache value tag shape is invalid.")

    @staticmethod
    def _fold(node: dict[str, Any]) -> int:
        fold = node.get("fold")
        if fold not in {0, 1} or isinstance(fold, bool):
            raise CacheConfigurationException("Redis cache fold value is invalid.")
        return int(fold)
