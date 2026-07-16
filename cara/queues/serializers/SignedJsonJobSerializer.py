"""Versioned, authenticated JSON wire format for AMQP jobs."""

from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import math
import re
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from cara.exceptions import QueueException
from cara.queues.contracts import ShouldQueue
from cara.queues.JobClassResolver import JobClassResolver
from cara.queues.PayloadLimits import MAX_AMQP_JOB_PAYLOAD_BYTES


class SignedJsonJobSerializer:
    """Serialize jobs as canonical, expiring, rotatable signed envelopes."""

    VERSION = 2
    MAX_PAYLOAD_BYTES = MAX_AMQP_JOB_PAYLOAD_BYTES
    DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60
    DEFAULT_MAX_AGE_SECONDS = 31 * 24 * 60 * 60
    DEFAULT_CLOCK_SKEW_SECONDS = 30
    MAX_JSON_DEPTH = 32
    _DOMAIN = b"cara.queue.job.v2\x00"
    _KEY_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    _FORBIDDEN_PAYLOAD_KEY_PATTERN = re.compile(
        r"(?:^|_)(?:password|passwd|secret|token|access_token|refresh_token|"
        r"api_key|private_key|authorization|cookie|credentials?)(?:$|_)",
        re.IGNORECASE,
    )
    _PRIORITIES = frozenset({"critical", "high", "default", "low"})
    _ENVELOPE_KEYS = frozenset(
        {
            "expires_at",
            "issued_at",
            "kid",
            "not_before",
            "payload",
            "signature",
        }
    )
    _SIGNED_ENVELOPE_KEYS = _ENVELOPE_KEYS - {"signature"}
    _PAYLOAD_KEYS = frozenset(
        {
            "_otel",
            "_tenant",
            "_tenant_mode",
            "args",
            "attempts",
            "callback",
            "created",
            "db_job_id",
            "dispatched_at",
            "job",
            "job_id",
            "priority",
            "queue",
            "replay_of",
            "timeout_seconds",
            "version",
        }
    )

    @classmethod
    def serialize(
        cls,
        payload: dict[str, Any],
        *,
        signing_key_id: str,
        signing_keys: Mapping[str, str | bytes],
        allowed_prefixes: tuple[str, ...] | list[str] | None = None,
        issued_at: Any | None = None,
        not_before: Any | None = None,
        expires_at: Any | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    ) -> bytes:
        job = payload.get("obj")
        if job is None:
            raise QueueException("Signed AMQP payload requires a job instance.")
        job_class = job if isinstance(job, type) else job.__class__
        if not issubclass(job_class, ShouldQueue):
            raise QueueException(
                f"Queued job {job_class.__module__}.{job_class.__name__} "
                "must implement ShouldQueue."
            )
        if not inspect.iscoroutinefunction(getattr(job_class, "handle", None)):
            raise QueueException(
                f"AMQP job {job_class.__name__}.handle must be async."
            )
        JobClassResolver.resolve(
            job_class.__module__,
            job_class.__name__,
            allowed_prefixes=allowed_prefixes,
        )

        callback = payload.get("callback", "handle")
        if callback != "handle":
            raise QueueException(
                "AMQP jobs may invoke only the queue contract's handle() callback."
            )
        init_kwargs = (
            payload.get("init_kwargs")
            if isinstance(job, type)
            else cls._dispatch_params(job)
        )
        signed_payload = cls._build_payload(
            job_descriptor={
                "module": job_class.__module__,
                "class": job_class.__name__,
                "kwargs": cls._json_value(init_kwargs or {}, path="job.kwargs"),
            },
            payload=payload,
        )
        cls._validate_job_tenancy(job_class, signed_payload)
        return cls._sign_payload(
            signed_payload,
            signing_key_id=signing_key_id,
            signing_keys=signing_keys,
            issued_at=issued_at,
            not_before=not_before,
            expires_at=expires_at,
            ttl_seconds=ttl_seconds,
            max_age_seconds=max_age_seconds,
        )

    @classmethod
    def serialize_replay(
        cls,
        verified_payload: Mapping[str, Any],
        *,
        new_job_id: str,
        new_db_job_id: int,
        signing_key_id: str,
        signing_keys: Mapping[str, str | bytes],
        issued_at: Any | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    ) -> bytes:
        """Create a new immutable delivery from a verified dead-letter payload."""
        cls._validate_verified_payload(dict(verified_payload))
        now = cls._epoch(issued_at, default_now=True)
        replay_payload = dict(verified_payload)
        replay_payload.update(
            {
                "attempts": 0,
                "created": datetime.fromtimestamp(now, tz=UTC).isoformat(),
                "db_job_id": cls._required_positive_int(
                    new_db_job_id,
                    "db_job_id",
                ),
                "dispatched_at": datetime.fromtimestamp(
                    now,
                    tz=UTC,
                ).isoformat(),
                "job_id": cls._required_uuid(new_job_id, "job_id"),
                "replay_of": cls._required_uuid(
                    verified_payload.get("job_id"),
                    "replay_of",
                ),
            }
        )
        return cls._sign_payload(
            replay_payload,
            signing_key_id=signing_key_id,
            signing_keys=signing_keys,
            issued_at=now,
            not_before=now,
            ttl_seconds=ttl_seconds,
            max_age_seconds=max_age_seconds,
        )

    @staticmethod
    def _dispatch_params(job: Any) -> dict[str, Any]:
        from cara.queues.Bus import Bus

        return Bus.get_dispatch_params(job)

    @classmethod
    def deserialize(
        cls,
        body: bytes | str,
        *,
        signing_keys: Mapping[str, str | bytes],
        allowed_prefixes: tuple[str, ...] | list[str] | None = None,
        now: Any | None = None,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
    ) -> dict[str, Any]:
        payload = cls.inspect(
            body,
            signing_keys=signing_keys,
            now=now,
            clock_skew_seconds=clock_skew_seconds,
        )
        return cls.deserialize_verified(
            payload,
            allowed_prefixes=allowed_prefixes,
        )

    @classmethod
    def deserialize_verified(
        cls,
        payload: Mapping[str, Any],
        *,
        allowed_prefixes: tuple[str, ...] | list[str] | None = None,
    ) -> dict[str, Any]:
        primitive = dict(payload)
        cls._validate_verified_payload(primitive)
        job = primitive["job"]
        job_class = JobClassResolver.resolve(
            job["module"],
            job["class"],
            allowed_prefixes=allowed_prefixes,
        )
        if not inspect.iscoroutinefunction(getattr(job_class, "handle", None)):
            raise QueueException(
                f"AMQP job {job_class.__name__}.handle must be async."
            )
        cls._validate_job_tenancy(job_class, primitive)
        return {
            "obj": job_class,
            "init_kwargs": job["kwargs"],
            "args": tuple(primitive["args"]),
            "callback": "handle",
            "created": primitive["created"],
            "job_id": primitive["job_id"],
            "db_job_id": primitive["db_job_id"],
            "timeout_seconds": primitive["timeout_seconds"],
            "attempts": primitive["attempts"],
            "_otel": primitive["_otel"],
            "_tenant": primitive["_tenant"],
            "_tenant_mode": primitive["_tenant_mode"],
            "queue": primitive["queue"],
            "priority": primitive["priority"],
            "dispatched_at": primitive["dispatched_at"],
            "replay_of": primitive["replay_of"],
        }

    @classmethod
    def inspect(
        cls,
        body: bytes | str | dict[str, Any],
        *,
        signing_keys: Mapping[str, str | bytes],
        now: Any | None = None,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
        allow_not_before: bool = False,
        allow_expired: bool = False,
    ) -> dict[str, Any]:
        """Verify signature/time bounds and return primitives without imports."""
        envelope = cls.inspect_envelope(
            body,
            signing_keys=signing_keys,
            now=now,
            clock_skew_seconds=clock_skew_seconds,
            max_age_seconds=max_age_seconds,
            allow_not_before=allow_not_before,
            allow_expired=allow_expired,
        )
        return envelope["payload"]

    @classmethod
    def inspect_envelope(
        cls,
        body: bytes | str | dict[str, Any],
        *,
        signing_keys: Mapping[str, str | bytes],
        now: Any | None = None,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
        allow_not_before: bool = False,
        allow_expired: bool = False,
    ) -> dict[str, Any]:
        envelope = cls._parse_envelope(body)
        kid = cls._required_key_id(envelope["kid"])
        if kid not in signing_keys:
            raise QueueException(f"Unknown AMQP signing key id: {kid!r}.")
        signature = envelope["signature"]
        if not isinstance(signature, str) or len(signature) != 64:
            raise QueueException("AMQP job signature has an invalid shape.")
        signed = {key: envelope[key] for key in cls._SIGNED_ENVELOPE_KEYS}
        expected = hmac.new(
            cls._key_bytes(signing_keys[kid]),
            cls._DOMAIN + cls._canonical_json(signed),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise QueueException("AMQP job signature verification failed.")

        payload = envelope["payload"]
        if not isinstance(payload, dict) or set(payload) != cls._PAYLOAD_KEYS:
            raise QueueException("AMQP job payload has an invalid shape.")
        cls._validate_verified_payload(payload)
        cls._validate_time_window(
            envelope,
            now=now,
            clock_skew_seconds=clock_skew_seconds,
            max_age_seconds=max_age_seconds,
            allow_not_before=allow_not_before,
            allow_expired=allow_expired,
        )
        return envelope

    @classmethod
    def canonical_envelope_bytes(
        cls,
        body: bytes | str | dict[str, Any],
    ) -> bytes:
        """Canonicalize an envelope for ledger hashing/JSONB round-trips."""
        return cls._canonical_json(cls._parse_envelope(body))

    @classmethod
    def canonical_envelope_sha256(
        cls,
        body: bytes | str | dict[str, Any],
    ) -> str:
        return hashlib.sha256(cls.canonical_envelope_bytes(body)).hexdigest()

    @classmethod
    def is_expired(
        cls,
        envelope: Mapping[str, Any],
        *,
        now: Any | None = None,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
    ) -> bool:
        """Apply the same expiry boundary used by envelope verification."""
        skew = cls._bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = cls._epoch(now, default_now=True)
        expiry = cls._non_negative_int(envelope.get("expires_at"), "expires_at")
        return expiry <= current - skew

    @classmethod
    def temporal_status(
        cls,
        envelope: Mapping[str, Any],
        *,
        now: Any | None = None,
        clock_skew_seconds: int = DEFAULT_CLOCK_SKEW_SECONDS,
    ) -> str:
        """Classify a cryptographically valid envelope independently."""
        skew = cls._bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = cls._epoch(now, default_now=True)
        available = cls._non_negative_int(
            envelope.get("not_before"),
            "not_before",
        )
        if cls.is_expired(
            envelope,
            now=current,
            clock_skew_seconds=skew,
        ):
            return "expired"
        if available > current + skew:
            return "not_ready"
        return "executable"

    @classmethod
    def _build_payload(
        cls,
        *,
        job_descriptor: dict[str, Any],
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "job": job_descriptor,
            "args": cls._json_value(payload.get("args", ()), path="args"),
            "callback": "handle",
            "created": cls._required_string(
                str(payload.get("created") or ""),
                "created",
            ),
            "job_id": cls._required_uuid(payload.get("job_id"), "job_id"),
            "db_job_id": cls._required_positive_int(
                payload.get("db_job_id"),
                "db_job_id",
            ),
            "timeout_seconds": cls._required_positive_int(
                payload.get("timeout_seconds"),
                "timeout_seconds",
            ),
            "attempts": cls._non_negative_int(payload.get("attempts", 0), "attempts"),
            "_otel": cls._json_value(payload.get("_otel") or {}, path="_otel"),
            "_tenant": cls._optional_positive_int(
                payload.get("_tenant"),
                "_tenant",
            ),
            "_tenant_mode": cls._required_string(
                payload.get("_tenant_mode"),
                "_tenant_mode",
            ),
            "queue": cls._bounded_string(payload.get("queue"), "queue", 100),
            "priority": cls._priority(payload.get("priority")),
            "dispatched_at": cls._required_string(
                payload.get("dispatched_at"),
                "dispatched_at",
            ),
            "replay_of": cls._optional_uuid(payload.get("replay_of"), "replay_of"),
        }

    @classmethod
    def _sign_payload(
        cls,
        payload: dict[str, Any],
        *,
        signing_key_id: str,
        signing_keys: Mapping[str, str | bytes],
        issued_at: Any | None,
        not_before: Any | None,
        expires_at: Any | None = None,
        ttl_seconds: int,
        max_age_seconds: int,
    ) -> bytes:
        kid = cls._required_key_id(signing_key_id)
        if kid not in signing_keys:
            raise QueueException("Active AMQP signing key id is absent from keyring.")
        issued = cls._epoch(issued_at, default_now=True)
        available = cls._epoch(not_before, default=issued)
        ttl = cls._bounded_seconds(ttl_seconds, "ttl_seconds", minimum=300)
        max_age = cls._bounded_seconds(
            max_age_seconds,
            "max_age_seconds",
            minimum=ttl,
        )
        expiry = cls._epoch(expires_at, default=available + ttl)
        temporal = {
            "issued_at": issued,
            "not_before": available,
            "expires_at": expiry,
        }
        cls._validate_temporal_order(temporal, max_age_seconds=max_age)

        signed = {
            "kid": kid,
            **temporal,
            "payload": payload,
        }
        signature = hmac.new(
            cls._key_bytes(signing_keys[kid]),
            cls._DOMAIN + cls._canonical_json(signed),
            hashlib.sha256,
        ).hexdigest()
        body = cls._canonical_json({**signed, "signature": signature})
        cls._require_size(len(body))
        return body

    @classmethod
    def _parse_envelope(
        cls,
        body: bytes | str | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(body, dict):
            envelope = body
            cls._require_size(len(cls._canonical_json(body)))
        else:
            if isinstance(body, bytes):
                cls._require_size(len(body))
                try:
                    raw = body.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise QueueException(
                        "AMQP job payload is not valid UTF-8 JSON."
                    ) from exc
            elif isinstance(body, str):
                cls._require_size(len(body.encode("utf-8")))
                raw = body
            else:
                raise QueueException("AMQP job payload must be bytes, text or JSON.")
            try:
                envelope = json.loads(raw, object_pairs_hook=cls._unique_object)
            except (json.JSONDecodeError, TypeError) as exc:
                raise QueueException("AMQP job payload is not valid JSON.") from exc
        if not isinstance(envelope, dict) or set(envelope) != cls._ENVELOPE_KEYS:
            raise QueueException("AMQP job envelope has an invalid shape.")
        return envelope

    @staticmethod
    def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise QueueException(f"AMQP job JSON contains duplicate key {key!r}.")
            result[key] = value
        return result

    @classmethod
    def _validate_time_window(
        cls,
        envelope: Mapping[str, Any],
        *,
        now: Any | None,
        clock_skew_seconds: int,
        max_age_seconds: int,
        allow_not_before: bool,
        allow_expired: bool,
    ) -> None:
        skew = cls._bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = cls._epoch(now, default_now=True)
        max_age = cls._bounded_seconds(
            max_age_seconds,
            "max_age_seconds",
            minimum=300,
        )
        cls._validate_temporal_order(
            envelope,
            max_age_seconds=max_age,
        )
        issued = envelope["issued_at"]
        available = envelope["not_before"]
        expiry = envelope["expires_at"]
        if issued > current + skew:
            raise QueueException("AMQP job envelope was issued in the future.")
        if not allow_not_before and available > current + skew:
            raise QueueException("AMQP job envelope is not yet executable.")
        if not allow_expired and expiry <= current - skew:
            raise QueueException("AMQP job envelope has expired.")

    @classmethod
    def _validate_temporal_order(
        cls,
        envelope: Mapping[str, Any],
        *,
        max_age_seconds: int,
    ) -> None:
        issued = cls._non_negative_int(envelope.get("issued_at"), "issued_at")
        available = cls._non_negative_int(envelope.get("not_before"), "not_before")
        expiry = cls._non_negative_int(envelope.get("expires_at"), "expires_at")
        if issued > available:
            raise QueueException("AMQP job not_before precedes issued_at.")
        if available >= expiry:
            raise QueueException("AMQP job expires_at must follow not_before.")
        if expiry - issued > max_age_seconds:
            raise QueueException("AMQP job envelope exceeds maximum age.")

    @classmethod
    def _validate_verified_payload(cls, payload: dict[str, Any]) -> None:
        if payload.get("version") != cls.VERSION:
            raise QueueException(
                f"Unsupported AMQP job envelope version: {payload.get('version')!r}"
            )
        job = payload.get("job")
        if not isinstance(job, dict) or set(job) != {"module", "class", "kwargs"}:
            raise QueueException("AMQP job descriptor has an invalid shape.")
        if not isinstance(job["module"], str) or not isinstance(job["class"], str):
            raise QueueException("AMQP job descriptor must use string names.")
        if not isinstance(job["kwargs"], dict):
            raise QueueException("AMQP job constructor kwargs must be an object.")
        if not isinstance(payload.get("args"), list):
            raise QueueException("AMQP job args must be an array.")
        if payload.get("callback") != "handle":
            raise QueueException("AMQP job callback must be handle.")
        cls._bounded_string(payload.get("queue"), "queue", 100)
        cls._priority(payload.get("priority"))
        cls._required_string(payload.get("dispatched_at"), "dispatched_at")
        cls._required_string(payload.get("created"), "created")
        cls._non_negative_int(payload.get("attempts"), "attempts")
        cls._required_positive_int(payload.get("db_job_id"), "db_job_id")
        cls._required_positive_int(
            payload.get("timeout_seconds"),
            "timeout_seconds",
        )
        tenant_id = cls._optional_positive_int(payload.get("_tenant"), "_tenant")
        tenant_mode = cls._required_string(payload.get("_tenant_mode"), "_tenant_mode")
        if tenant_mode == "tenant":
            if tenant_id is None:
                raise QueueException("Tenant AMQP jobs require a signed tenant id.")
        elif tenant_mode == "central":
            if tenant_id is not None:
                raise QueueException("Central AMQP jobs cannot carry a tenant id.")
        else:
            raise QueueException("AMQP job tenant mode must be tenant or central.")
        cls._required_uuid(payload.get("job_id"), "job_id")
        cls._optional_uuid(payload.get("replay_of"), "replay_of")
        cls._json_value(job["kwargs"], path="job.kwargs")
        cls._json_value(payload["args"], path="args")
        cls._json_value(payload.get("_otel"), path="_otel")

    @staticmethod
    def _validate_job_tenancy(job_class: type, payload: Mapping[str, Any]) -> None:
        is_central_job = bool(getattr(job_class, "central_job", False))
        mode = payload.get("_tenant_mode")
        if is_central_job and mode != "central":
            raise QueueException(
                f"Central job {job_class.__name__} requires signed central mode."
            )
        if not is_central_job and mode != "tenant":
            raise QueueException(
                f"Ordinary job {job_class.__name__} requires signed tenant mode."
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
            raise QueueException(f"AMQP job payload is not canonical JSON: {exc}") from exc

    @classmethod
    def _json_value(
        cls,
        value: Any,
        *,
        path: str,
        depth: int = 0,
    ) -> Any:
        if depth > cls.MAX_JSON_DEPTH:
            raise QueueException(f"{path} exceeds the maximum JSON depth.")
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise QueueException(f"{path} contains a non-finite float.")
            return value
        if isinstance(value, (list, tuple)):
            return [
                cls._json_value(
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
                if cls._FORBIDDEN_PAYLOAD_KEY_PATTERN.search(key):
                    raise QueueException(
                        f"{path} contains forbidden secret-bearing key {key!r}; "
                        "queued jobs must carry durable IDs, not credentials."
                    )
                normalized[key] = cls._json_value(
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
    def _key_bytes(signing_key: str | bytes) -> bytes:
        key = signing_key.encode("utf-8") if isinstance(signing_key, str) else signing_key
        if not isinstance(key, bytes) or len(key) < 32:
            raise QueueException("AMQP signing key must contain at least 32 bytes.")
        return key

    @classmethod
    def _require_size(cls, size: int) -> None:
        if size > cls.MAX_PAYLOAD_BYTES:
            raise QueueException(
                "AMQP job payload exceeds maximum wire size "
                f"({size} > {cls.MAX_PAYLOAD_BYTES} bytes)."
            )

    @staticmethod
    def _required_string(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value:
            raise QueueException(f"AMQP job field {field!r} must be a non-empty string.")
        return value

    @classmethod
    def _required_key_id(cls, value: Any) -> str:
        key_id = cls._required_string(value, "kid")
        if not cls._KEY_ID_PATTERN.fullmatch(key_id):
            raise QueueException("AMQP signing key id has an invalid format.")
        return key_id

    @classmethod
    def _required_uuid(cls, value: Any, field: str) -> str:
        text = cls._required_string(value, field)
        try:
            normalized = str(uuid.UUID(text))
        except (ValueError, AttributeError) as exc:
            raise QueueException(
                f"AMQP job field {field!r} must be a canonical UUID."
            ) from exc
        if text != normalized:
            raise QueueException(
                f"AMQP job field {field!r} must be a canonical UUID."
            )
        return text

    @classmethod
    def _optional_uuid(cls, value: Any, field: str) -> str | None:
        if value is None:
            return None
        return cls._required_uuid(value, field)

    @classmethod
    def _bounded_string(cls, value: Any, field: str, maximum: int) -> str:
        text = cls._required_string(value, field)
        if len(text) > maximum:
            raise QueueException(
                f"AMQP job field {field!r} exceeds {maximum} characters."
            )
        return text

    @classmethod
    def _priority(cls, value: Any) -> str:
        priority = cls._required_string(value, "priority")
        if priority not in cls._PRIORITIES:
            raise QueueException(
                "AMQP job priority must be critical, high, default or low."
            )
        return priority

    @staticmethod
    def _optional_positive_int(value: Any, field: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise QueueException(
                f"AMQP job field {field!r} must be a positive integer or null."
            )
        return value

    @staticmethod
    def _required_positive_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise QueueException(
                f"AMQP job field {field!r} must be a positive integer."
            )
        return value

    @staticmethod
    def _non_negative_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise QueueException(
                f"AMQP job field {field!r} must be a non-negative integer."
            )
        return value

    @staticmethod
    def _bounded_seconds(
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
    def _epoch(
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
