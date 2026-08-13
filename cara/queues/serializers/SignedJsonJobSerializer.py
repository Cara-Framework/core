"""Versioned, authenticated JSON wire format for AMQP jobs."""

from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from cara.exceptions import QueueException
from cara.queues.Bus import Bus
from cara.queues.contracts import ShouldQueue, UniqueJob
from cara.queues.JobClassResolver import JobClassResolver
from cara.queues.PayloadLimits import MAX_AMQP_JOB_PAYLOAD_BYTES

from ._SignedJsonValues import _SignedJsonValues


class SignedJsonJobSerializer:
    """Serialize jobs as canonical, expiring, rotatable signed envelopes."""

    VERSION = 4
    MAX_PAYLOAD_BYTES = MAX_AMQP_JOB_PAYLOAD_BYTES
    DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60
    DEFAULT_MAX_AGE_SECONDS = 31 * 24 * 60 * 60
    DEFAULT_CLOCK_SKEW_SECONDS = 30
    MAX_JSON_DEPTH = 32
    _DOMAIN = b"cara.queue.job.v3\x00"
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
            "throttle_attempts",
            "unique_key",
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
            raise QueueException(f"AMQP job {job_class.__name__}.handle must be async.")
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
                "kwargs": _SIGNED_JSON_VALUES.json_value(
                    init_kwargs or {}, path="job.kwargs"
                ),
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
        now = _SIGNED_JSON_VALUES.epoch(issued_at, default_now=True)
        replay_payload = dict(verified_payload)
        replay_payload.update(
            {
                "attempts": 0,
                "throttle_attempts": 0,
                "created": datetime.fromtimestamp(now, tz=UTC).isoformat(),
                "db_job_id": _SIGNED_JSON_VALUES.required_positive_int(
                    new_db_job_id,
                    "db_job_id",
                ),
                "dispatched_at": datetime.fromtimestamp(
                    now,
                    tz=UTC,
                ).isoformat(),
                "job_id": _SIGNED_JSON_VALUES.required_uuid(new_job_id, "job_id"),
                "replay_of": _SIGNED_JSON_VALUES.required_uuid(
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
            raise QueueException(f"AMQP job {job_class.__name__}.handle must be async.")
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
            "throttle_attempts": primitive["throttle_attempts"],
            "_otel": primitive["_otel"],
            "_tenant": primitive["_tenant"],
            "_tenant_mode": primitive["_tenant_mode"],
            "queue": primitive["queue"],
            "priority": primitive["priority"],
            "dispatched_at": primitive["dispatched_at"],
            "replay_of": primitive["replay_of"],
            "unique_key": primitive["unique_key"],
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
        kid = _SIGNED_JSON_VALUES.required_key_id(envelope["kid"])
        if kid not in signing_keys:
            raise QueueException(f"Unknown AMQP signing key id: {kid!r}.")
        signature = envelope["signature"]
        if not isinstance(signature, str) or len(signature) != 64:
            raise QueueException("AMQP job signature has an invalid shape.")
        signed = {key: envelope[key] for key in cls._SIGNED_ENVELOPE_KEYS}
        expected = hmac.new(
            _SIGNED_JSON_VALUES.key_bytes(signing_keys[kid]),
            cls._DOMAIN + _SIGNED_JSON_VALUES.canonical_json(signed),
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
        return _SIGNED_JSON_VALUES.canonical_json(cls._parse_envelope(body))

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
        skew = _SIGNED_JSON_VALUES.bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = _SIGNED_JSON_VALUES.epoch(now, default_now=True)
        expiry = _SIGNED_JSON_VALUES.non_negative_int(
            envelope.get("expires_at"), "expires_at"
        )
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
        skew = _SIGNED_JSON_VALUES.bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = _SIGNED_JSON_VALUES.epoch(now, default_now=True)
        available = _SIGNED_JSON_VALUES.non_negative_int(
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
            "args": _SIGNED_JSON_VALUES.json_value(payload.get("args", ()), path="args"),
            "callback": "handle",
            "created": _SIGNED_JSON_VALUES.required_string(
                str(payload.get("created") or ""),
                "created",
            ),
            "job_id": _SIGNED_JSON_VALUES.required_uuid(payload.get("job_id"), "job_id"),
            "db_job_id": _SIGNED_JSON_VALUES.required_positive_int(
                payload.get("db_job_id"),
                "db_job_id",
            ),
            "timeout_seconds": _SIGNED_JSON_VALUES.required_positive_int(
                payload.get("timeout_seconds"),
                "timeout_seconds",
            ),
            "attempts": _SIGNED_JSON_VALUES.non_negative_int(
                payload.get("attempts", 0), "attempts"
            ),
            "throttle_attempts": _SIGNED_JSON_VALUES.non_negative_int(
                payload.get("throttle_attempts", 0),
                "throttle_attempts",
            ),
            "_otel": _SIGNED_JSON_VALUES.json_value(
                payload.get("_otel") or {}, path="_otel"
            ),
            "_tenant": _SIGNED_JSON_VALUES.optional_positive_int(
                payload.get("_tenant"),
                "_tenant",
            ),
            "_tenant_mode": _SIGNED_JSON_VALUES.required_string(
                payload.get("_tenant_mode"),
                "_tenant_mode",
            ),
            "queue": _SIGNED_JSON_VALUES.bounded_string(
                payload.get("queue"), "queue", 100
            ),
            "priority": _SIGNED_JSON_VALUES.priority(payload.get("priority")),
            "dispatched_at": _SIGNED_JSON_VALUES.required_string(
                payload.get("dispatched_at"),
                "dispatched_at",
            ),
            "replay_of": _SIGNED_JSON_VALUES.optional_uuid(
                payload.get("replay_of"), "replay_of"
            ),
            "unique_key": (
                _SIGNED_JSON_VALUES.bounded_string(
                    payload.get("unique_key"),
                    "unique_key",
                    500,
                )
                if payload.get("unique_key") is not None
                else None
            ),
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
        kid = _SIGNED_JSON_VALUES.required_key_id(signing_key_id)
        if kid not in signing_keys:
            raise QueueException("Active AMQP signing key id is absent from keyring.")
        issued = _SIGNED_JSON_VALUES.epoch(issued_at, default_now=True)
        available = _SIGNED_JSON_VALUES.epoch(not_before, default=issued)
        ttl = _SIGNED_JSON_VALUES.bounded_seconds(ttl_seconds, "ttl_seconds", minimum=300)
        max_age = _SIGNED_JSON_VALUES.bounded_seconds(
            max_age_seconds,
            "max_age_seconds",
            minimum=ttl,
        )
        expiry = _SIGNED_JSON_VALUES.epoch(expires_at, default=available + ttl)
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
            _SIGNED_JSON_VALUES.key_bytes(signing_keys[kid]),
            cls._DOMAIN + _SIGNED_JSON_VALUES.canonical_json(signed),
            hashlib.sha256,
        ).hexdigest()
        body = _SIGNED_JSON_VALUES.canonical_json({**signed, "signature": signature})
        _SIGNED_JSON_VALUES.require_size(len(body), maximum=cls.MAX_PAYLOAD_BYTES)
        return body

    @classmethod
    def _parse_envelope(
        cls,
        body: bytes | str | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(body, dict):
            envelope = body
            _SIGNED_JSON_VALUES.require_size(
                len(_SIGNED_JSON_VALUES.canonical_json(body)),
                maximum=cls.MAX_PAYLOAD_BYTES,
            )
        else:
            if isinstance(body, bytes):
                _SIGNED_JSON_VALUES.require_size(
                    len(body),
                    maximum=cls.MAX_PAYLOAD_BYTES,
                )
                try:
                    raw = body.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise QueueException(
                        "AMQP job payload is not valid UTF-8 JSON."
                    ) from exc
            elif isinstance(body, str):
                _SIGNED_JSON_VALUES.require_size(
                    len(body.encode("utf-8")),
                    maximum=cls.MAX_PAYLOAD_BYTES,
                )
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
        skew = _SIGNED_JSON_VALUES.bounded_seconds(
            clock_skew_seconds,
            "clock_skew_seconds",
            minimum=0,
            maximum=300,
        )
        current = _SIGNED_JSON_VALUES.epoch(now, default_now=True)
        max_age = _SIGNED_JSON_VALUES.bounded_seconds(
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
        issued = _SIGNED_JSON_VALUES.non_negative_int(
            envelope.get("issued_at"), "issued_at"
        )
        available = _SIGNED_JSON_VALUES.non_negative_int(
            envelope.get("not_before"), "not_before"
        )
        expiry = _SIGNED_JSON_VALUES.non_negative_int(
            envelope.get("expires_at"), "expires_at"
        )
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
        _SIGNED_JSON_VALUES.bounded_string(payload.get("queue"), "queue", 100)
        _SIGNED_JSON_VALUES.priority(payload.get("priority"))
        _SIGNED_JSON_VALUES.required_string(payload.get("dispatched_at"), "dispatched_at")
        _SIGNED_JSON_VALUES.required_string(payload.get("created"), "created")
        _SIGNED_JSON_VALUES.non_negative_int(payload.get("attempts"), "attempts")
        _SIGNED_JSON_VALUES.non_negative_int(
            payload.get("throttle_attempts"),
            "throttle_attempts",
        )
        _SIGNED_JSON_VALUES.required_positive_int(payload.get("db_job_id"), "db_job_id")
        _SIGNED_JSON_VALUES.required_positive_int(
            payload.get("timeout_seconds"),
            "timeout_seconds",
        )
        tenant_id = _SIGNED_JSON_VALUES.optional_positive_int(
            payload.get("_tenant"), "_tenant"
        )
        tenant_mode = _SIGNED_JSON_VALUES.required_string(
            payload.get("_tenant_mode"), "_tenant_mode"
        )
        if tenant_mode == "tenant":
            if tenant_id is None:
                raise QueueException("Tenant AMQP jobs require a signed tenant id.")
        elif tenant_mode == "central":
            if tenant_id is not None:
                raise QueueException("Central AMQP jobs cannot carry a tenant id.")
        else:
            raise QueueException("AMQP job tenant mode must be tenant or central.")
        _SIGNED_JSON_VALUES.required_uuid(payload.get("job_id"), "job_id")
        _SIGNED_JSON_VALUES.optional_uuid(payload.get("replay_of"), "replay_of")
        if payload.get("unique_key") is not None:
            _SIGNED_JSON_VALUES.bounded_string(
                payload.get("unique_key"),
                "unique_key",
                500,
            )
        _SIGNED_JSON_VALUES.json_value(job["kwargs"], path="job.kwargs")
        _SIGNED_JSON_VALUES.json_value(payload["args"], path="args")
        _SIGNED_JSON_VALUES.json_value(payload.get("_otel"), path="_otel")

    @staticmethod
    def _validate_job_tenancy(job_class: type, payload: Mapping[str, Any]) -> None:
        is_central_job = bool(getattr(job_class, "central_job", False))
        unique_key = payload.get("unique_key")
        if issubclass(job_class, UniqueJob):
            if unique_key is None:
                raise QueueException(
                    f"Unique job {job_class.__name__} requires a signed unique key."
                )
        elif unique_key is not None:
            raise QueueException(
                f"Non-unique job {job_class.__name__} cannot carry a unique key."
            )
        mode = payload.get("_tenant_mode")
        if is_central_job and mode != "central":
            raise QueueException(
                f"Central job {job_class.__name__} requires signed central mode."
            )
        if not is_central_job and mode != "tenant":
            raise QueueException(
                f"Ordinary job {job_class.__name__} requires signed tenant mode."
            )


_SIGNED_JSON_VALUES = _SignedJsonValues(
    key_id_pattern=SignedJsonJobSerializer._KEY_ID_PATTERN,
    forbidden_key_pattern=SignedJsonJobSerializer._FORBIDDEN_PAYLOAD_KEY_PATTERN,
    priorities=SignedJsonJobSerializer._PRIORITIES,
    max_json_depth=SignedJsonJobSerializer.MAX_JSON_DEPTH,
)
