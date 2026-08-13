"""Canonical JSON primitive validation for signed queue envelopes."""

from __future__ import annotations

import json
import math
import time
import uuid
from datetime import datetime
from re import Pattern
from typing import Any

from cara.exceptions import QueueException


class _SignedJsonValues:
    def __init__(
        self,
        *,
        key_id_pattern: Pattern[str],
        forbidden_key_pattern: Pattern[str],
        priorities: frozenset[str],
        max_json_depth: int,
    ) -> None:
        self._key_id_pattern = key_id_pattern
        self._forbidden_key_pattern = forbidden_key_pattern
        self._priorities = priorities
        self._max_json_depth = max_json_depth

    @staticmethod
    def canonical_json(value: Any) -> bytes:
        try:
            return json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise QueueException(
                f"AMQP job payload is not canonical JSON: {exc}"
            ) from exc

    def json_value(
        self,
        value: Any,
        *,
        path: str,
        depth: int = 0,
    ) -> Any:
        if depth > self._max_json_depth:
            raise QueueException(f"{path} exceeds the maximum JSON depth.")
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise QueueException(f"{path} contains a non-finite float.")
            return value
        if isinstance(value, (list, tuple)):
            return [
                self.json_value(
                    item,
                    path=f"{path}[{index}]",
                    depth=depth + 1,
                )
                for index, item in enumerate(value)
            ]
        if isinstance(value, dict):
            normalized = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise QueueException(f"{path} contains a non-string object key.")
                if self._forbidden_key_pattern.search(key):
                    raise QueueException(
                        f"{path} contains forbidden secret-bearing key {key!r}; "
                        "queued jobs must carry durable IDs, not credentials."
                    )
                normalized[key] = self.json_value(
                    item,
                    path=f"{path}.{key}",
                    depth=depth + 1,
                )
            return normalized
        raise QueueException(
            f"{path} contains unsupported {type(value).__name__}; "
            "queued job constructors must use JSON primitives."
        )

    @staticmethod
    def key_bytes(signing_key: str | bytes) -> bytes:
        key = signing_key.encode("utf-8") if isinstance(signing_key, str) else signing_key
        if not isinstance(key, bytes) or len(key) < 32:
            raise QueueException("AMQP signing key must contain at least 32 bytes.")
        return key

    @staticmethod
    def require_size(size: int, *, maximum: int) -> None:
        if size > maximum:
            raise QueueException(
                f"AMQP job payload exceeds maximum wire size ({size} > {maximum} bytes)."
            )

    @staticmethod
    def required_string(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value:
            raise QueueException(f"AMQP job field {field!r} must be a non-empty string.")
        return value

    def required_key_id(self, value: Any) -> str:
        key_id = self.required_string(value, "kid")
        if not self._key_id_pattern.fullmatch(key_id):
            raise QueueException("AMQP signing key id has an invalid format.")
        return key_id

    def required_uuid(self, value: Any, field: str) -> str:
        text = self.required_string(value, field)
        try:
            normalized = str(uuid.UUID(text))
        except (ValueError, AttributeError) as exc:
            raise QueueException(
                f"AMQP job field {field!r} must be a canonical UUID."
            ) from exc
        if text != normalized:
            raise QueueException(f"AMQP job field {field!r} must be a canonical UUID.")
        return text

    def optional_uuid(self, value: Any, field: str) -> str | None:
        if value is None:
            return None
        return self.required_uuid(value, field)

    def bounded_string(self, value: Any, field: str, maximum: int) -> str:
        text = self.required_string(value, field)
        if len(text) > maximum:
            raise QueueException(
                f"AMQP job field {field!r} exceeds {maximum} characters."
            )
        return text

    def priority(self, value: Any) -> str:
        priority = self.required_string(value, "priority")
        if priority not in self._priorities:
            raise QueueException(
                "AMQP job priority must be critical, high, default or low."
            )
        return priority

    @staticmethod
    def optional_positive_int(value: Any, field: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise QueueException(
                f"AMQP job field {field!r} must be a positive integer or null."
            )
        return value

    @staticmethod
    def required_positive_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise QueueException(f"AMQP job field {field!r} must be a positive integer.")
        return value

    @staticmethod
    def non_negative_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise QueueException(
                f"AMQP job field {field!r} must be a non-negative integer."
            )
        return value

    @staticmethod
    def bounded_seconds(
        value: Any,
        field: str,
        *,
        minimum: int,
        maximum: int | None = None,
    ) -> int:
        if isinstance(value, bool):
            raise QueueException(f"{field} must be an integer.")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise QueueException(f"{field} must be an integer.") from exc
        if parsed < minimum or (maximum is not None and parsed > maximum):
            bound = (
                f"between {minimum} and {maximum}"
                if maximum is not None
                else f"at least {minimum}"
            )
            raise QueueException(f"{field} must be {bound}.")
        return parsed

    @staticmethod
    def epoch(
        value: Any | None,
        *,
        default: int | None = None,
        default_now: bool = False,
    ) -> int:
        if value is None:
            if default_now:
                return int(time.time())
            if default is not None:
                return default
            raise QueueException("AMQP job timestamp is required.")
        if isinstance(value, bool):
            raise QueueException("AMQP job timestamp must be an epoch integer.")
        if isinstance(value, (int, float)):
            parsed = int(value)
        elif isinstance(value, datetime):
            if value.tzinfo is None:
                raise QueueException("AMQP job timestamps must include a timezone.")
            parsed = int(value.timestamp())
        elif hasattr(value, "timestamp") and callable(value.timestamp):
            parsed = int(value.timestamp())
        else:
            raise QueueException("AMQP job timestamp must be timezone-aware.")
        if parsed < 0:
            raise QueueException("AMQP job timestamp cannot be negative.")
        return parsed
