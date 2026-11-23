"""A job's idempotency identity — its key, its parameters, its log name.

Everything here answers one question: "which dispatches are the SAME unit of
work?". ``MakesIdempotentBase`` asks it once per run and then spends the
answer on the result cache, the lease and the cooldown; nothing in this file
talks to the cache. It lived inside the orchestrator until 2026-11-23, when
the lease winner's re-read took that file past the source-shape line budget
and the identity half, which has no state and no collaborator, earned its own
(the way ``ClaimsSourceCooldown`` did before it).

Apps override these per job: a pinned ``idempotency_params`` tuple, a
``get_job_parameters`` that orders its canonical fields, a
``generate_idempotency_key`` for a scheduled sweep.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from cara.context import Tenancy
from cara.queues.idempotency.IdempotencyParams import normalize_idempotency_param

__all__ = ["DerivesIdempotencyKey"]


class DerivesIdempotencyKey:
    """Derive the idempotency key and identity of the job it is mixed into."""

    #: Queue-runner internals: where a job is sent and how often it was
    #: tried. Never part of a key, declared or not.
    _RUNNER_ATTRS = frozenset({"queue", "routing_key", "attempts", "job_id"})

    #: What the framework itself writes on, or reads off, a job INSTANCE to
    #: deliver or gate ONE dispatch of it. None of it says what the work is:
    #:
    #: * one delivery, one run: ``job_tracking_id`` (the delivery UUID — the
    #:   worker stamps it before the body runs, ``PendingDispatch`` at
    #:   dispatch) and ``is_cancelled`` (``CancellableJob``);
    #: * the delivery priority the queue driver reads off the instance:
    #:   ``priority`` (``Trackable`` stamps a default) and ``job_priority``;
    #: * the gate's own switches: ``force``, ``bypass_idempotency_result``,
    #:   ``retry_on_idempotency_overlap`` and ``idempotency_cache_results``
    #:   (the scheduler stamps that one on every tick it runs).
    #:
    #: The default scan read all of it as identity until 2026-11-23. Every
    #: queued delivery of a job without ``idempotency_params`` — a duplicate
    #: dispatch, a redelivery, a retry child — hashed to a key of its own, so
    #: the result cache never answered and the lease never excluded, while a
    #: sync run of the same work (no delivery, no stamp) derived another,
    #: stable key. A scheduler tick keyed apart from a queued run of the same
    #: job, and a forced run apart from the run whose lease it must respect.
    #: A stamp the framework adds per delivery is underscore-private (the
    #: worker's ``_current_attempt``, ``_db_job_id``, …) or it is named here.
    #:
    #: Left out of the DEFAULT scan only. A name a job lists in
    #: ``idempotency_params`` is that job's own statement of identity.
    _DISPATCH_ATTRS = frozenset(
        {
            "job_tracking_id",
            "is_cancelled",
            "priority",
            "job_priority",
            "force",
            "bypass_idempotency_result",
            "retry_on_idempotency_overlap",
            "idempotency_cache_results",
        }
    )

    def generate_idempotency_key(self) -> str:
        """Idempotency key from class + parameters + the ARMED tenancy: one
        job run for two workspaces is two units of work, never one shared
        result cache and lease (it made jobs carry a second ``tenant_id``)."""
        tenancy = "central" if Tenancy.is_central() else Tenancy.id()
        job_data = {
            "job_class": self.__class__.__name__,
            "parameters": self.get_job_parameters(),
            "tenancy": None if tenancy is None else str(tenancy),
            "version": "1.0",
        }
        key_string = json.dumps(job_data, sort_keys=True)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def get_job_parameters(self) -> dict[str, Any]:
        """Return job parameters for idempotency key generation.

        Default behaviour: include every public, non-callable instance
        attribute that describes the WORK. Queue-runner internals
        (``_RUNNER_ATTRS``) and what delivers or gates one dispatch
        (``_DISPATCH_ATTRS``) are not identity, so a sync run, a scheduler
        tick, a queued delivery and its retries of one unit of work share
        one key.

        Subclasses can pin the dedup surface by setting a class-level
        ``idempotency_params`` tuple — only the listed attributes
        contribute to the hash. This is the right shape when a job's
        identity is "what entity does this touch" rather than "what
        bag of optional kwargs was it dispatched with":

            class ConsolidateJob(BaseJob):
                idempotency_params = ("record_id",)

        Without the whitelist, ``ConsolidateJob(record_id=42)``
        and ``ConsolidateJob(record_id=42, extra_data={...})``
        produce different keys, both acquire different locks, and both
        write to the same product row concurrently — the exact race
        the lock exists to prevent.
        """
        whitelist = getattr(self, "idempotency_params", None)

        params: dict[str, Any] = {}
        for key, value in vars(self).items():
            if key.startswith("_") or key in self._RUNNER_ATTRS:
                continue
            if callable(value):
                continue
            if whitelist is None:
                if key in self._DISPATCH_ATTRS:
                    continue
            elif key not in whitelist:
                continue
            normalized = self._normalize_param_value(value, strict=whitelist is not None)
            if normalized is not None:
                params[key] = normalized
        return params

    def _normalize_param_value(self, value: Any, *, strict: bool = False) -> Any:
        """One parameter's key form (``IdempotencyParams``)."""
        try:
            return normalize_idempotency_param(value, strict=strict)
        except TypeError as exc:
            raise TypeError(f"{self.__class__.__name__}: {exc}") from None

    def get_job_identifier(self) -> str:
        """Human-readable identifier used in log lines."""
        params = self.get_job_parameters()
        if "product_id" in params:
            return f"{self.__class__.__name__}(product_id={params['product_id']})"
        elif "source" in params:
            return f"{self.__class__.__name__}(source={params['source']})"
        return f"{self.__class__.__name__}({params})"
