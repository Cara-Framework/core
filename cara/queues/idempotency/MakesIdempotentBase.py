"""Generic flow-level idempotency mixin (Laravel-trait-shaped).

The framework owns the cache + lock + key-generation primitives so
every cara app gets the same crash-safe "this job ran already, skip
it" story without re-rolling its own. Apps subclass and override
hooks to plug in domain-specific lifecycle gating, source cooldowns,
and metric emission.

Hooks subclasses MAY override (defaults are deliberately pass-through):

* ``get_lifecycle_step()`` — return a short stage name (``"validated"``
  etc.) the app's lifecycle store records on success. Default: ``None``
  (no lifecycle gating).
* ``should_execute_based_on_lifecycle()`` — return False to skip when
  the lifecycle store says this stage already ran for this entity.
  Default: ``True`` (always execute).
* ``should_collect_again()`` — return False when a domain cooldown
  (per-source / per-keyword) is active. Default: ``True``.
* ``_emit_idempotency_metric(outcome)`` — fire a Prometheus counter
  for ``{collision,locked,lifecycle_skip,fresh}``. Default: no-op.
* ``_emit_cache_op_metric(operation, outcome)`` — fire a
  ``{get,put} × {hit,miss,…}`` counter. Default: no-op.

Subclasses MUST set ``_idempotency_key`` indirectly by calling
``wrap_with_idempotency`` (it generates from
``get_job_parameters`` + ``__class__.__name__``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pendulum

from cara.configuration import config
from cara.exceptions import IdempotencyOverlapException
from cara.facades import Cache, Log


class MakesIdempotentBase:
    """Laravel-style trait for flow-level idempotency (opt-in per job).

    Subclasses define their own ``handle()`` and run the body through
    :meth:`wrap_with_idempotency` so cache / locks / lifecycle checks
    apply consistently.

    Features:
        * SHA-256 idempotency key derived from class name + job parameters
        * Result cache for de-dup of completed runs —
          ``IDEMPOTENCY_CACHE_TTL`` for payload-bearing results,
          ``IDEMPOTENCY_NONE_TTL`` for bare-``None`` completions (a
          None is indistinguishable from a claim-miss no-op)
        * Distributed lock (``JOB_LOCK_TTL`` seconds) — concurrent
          duplicate dispatches converge on a single execution
        * Hooks for app-defined lifecycle gating + per-source cooldowns
        * Optional metric emission via subclass hooks
    """

    #: Cache TTL for result cache (24 hours) — jobs that RETURN a value.
    IDEMPOTENCY_CACHE_TTL = 24 * 60 * 60

    #: Cache TTL for a ``None`` result (5 minutes). A bare-None
    #: completion is AMBIGUOUS: fire-and-forget jobs return None on a
    #: true success, but state-machine jobs (outbox pushes with a CAS
    #: claim) ALSO return None when the run was a no-op because another
    #: actor owned the entity — a claim-miss. Caching that None for the
    #: full 24h window poisoned every later re-dispatch of the same
    #: identity (a re-dispatched job "completed" in 1ms without
    #: reaching the external system). A None result therefore only dedupes
    #: the realistic duplicate-dispatch horizon — double scheduler
    #: ticks, racing envelopes, AMQP redelivery, the
    #: ``wait_for_completion`` window — while a re-dispatch driven by a
    #: real state change (sweep re-queues, operator retries) lands
    #: after expiry and RUNS. Results that carry a payload keep the
    #: full window: a value proves the work happened.
    IDEMPOTENCY_NONE_TTL = 5 * 60

    #: Lock TTL for active jobs (30 minutes).
    JOB_LOCK_TTL = 30 * 60

    #: Sentinel that encodes a cached ``None`` return value. Without
    #: this, ``cache_result(None)`` writes ``None`` and
    #: ``get_cached_result()`` can't tell "ran successfully and
    #: returned nothing" from "never ran" — every subsequent dispatch
    #: of a None-returning job (every fire-and-forget pipeline step)
    #: re-executed the work it was supposed to skip. Stored as a
    #: stable string literal so JSON / pickle Cache drivers
    #: round-trip it identically. Lives for ``IDEMPOTENCY_NONE_TTL``
    #: only — see that knob for why a None must not poison 24h.
    _NONE_SENTINEL = "__cara_idempotent_none__"

    #: Whether this job participates in the 24h *result cache* dedup.
    #: Per-entity pipeline jobs keep this True (re-dispatching the same
    #: listing_id must not re-run the work). RECURRING SCHEDULED jobs set
    #: it False — they hash to one stable key (no per-run params), so the
    #: result cache would dedupe every tick after the first into a single
    #: run per IDEMPOTENCY_CACHE_TTL (24h). The scheduler flips this off
    #: for scheduled invocations. Overlap is still guarded by the job lock
    #: (and any WithoutOverlapping / cross-process lock the job declares).
    idempotency_cache_results = True

    #: Durable intent jobs opt in when returning another dispatch's cached
    #: result would strand their own database row. An overlap is surfaced as
    #: a throttle so the queue redelivers after the current owner releases the
    #: channel-grain lease; the two callbacks never run concurrently.
    retry_on_idempotency_overlap = False

    #: Cache-key namespace for the per-source poll cooldown.
    COOLDOWN_KEY_PREFIX = "collection_cooldown:"

    #: Cache-key namespace for the result cache (:meth:`_result_key`).
    RESULT_KEY_PREFIX = "job_result:"

    #: Cache-key namespace for the exclusive job lease (:meth:`_lock_key`).
    LOCK_KEY_PREFIX = "job_lock:"

    #: Cache-key namespace for the monotonic owner fence
    #: (:meth:`_fence_key`).
    FENCE_KEY_PREFIX = "job_fence:"

    #: Namespaces a schema reset MUST clear, as the single source that
    #: ``migrate:reset`` reads instead of restating the vocabulary.
    #:
    #: The result cache and the job lease are hashed from the job class
    #: plus its entity-id arguments. A fresh schema restarts every
    #: sequence at 1, so listing id 5 after a reset collides with a stale
    #: entry belonging to a DIFFERENT entity that held id 5 before it —
    #: the job is served from cache and its downstream dispatch never
    #: fires, silently stalling the pipeline.
    #:
    #: :attr:`FENCE_KEY_PREFIX` is deliberately ABSENT. The fence is a
    #: monotonic counter, never a gate: it stamps each lease with a
    #: strictly increasing owner token so a resurrected zombie holder can
    #: be recognised as stale. Flushing it would really delete the
    #: counters (``RedisCacheDriver.forget_pattern`` scans the counter
    #: namespace too) and restart them at 1, minting fence numbers that
    #: have already been issued — reuse is precisely the failure the
    #: fence exists to prevent. A too-high fence after a reset is
    #: harmless; a reused one is not.
    RESET_FLUSHABLE_KEY_PREFIXES = (RESULT_KEY_PREFIX, LOCK_KEY_PREFIX)

    #: Per-source cooldown windows in minutes, e.g. ``{"reports": 5}``. A
    #: source absent from the map falls back to
    #: ``config("jobs.source_cooldown_minutes")`` and then to
    #: :attr:`default_source_cooldown_minutes`.
    source_cooldown_minutes: dict[str, int] = {}

    #: Cooldown for sources the map does not name.
    default_source_cooldown_minutes = 15

    #: Job attributes whose truthy values, in this order, make one poll
    #: distinct from another. They join the cooldown key after the source.
    cooldown_grain_attrs: tuple[str, ...] = ()

    #: When True, a job that resolves NO grain is never throttled. Set this
    #: wherever a source-only key would be a fleet-wide claim (multi-entity
    #: deployments); leave it False where the source alone IS the identity.
    cooldown_requires_grains = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._idempotency_key: str | None = None
        self._idempotency_lock_value: dict[str, Any] | None = None
        self._idempotency_fence: int | None = None

    # ── Public orchestrator ─────────────────────────────────────────

    async def wrap_with_idempotency(
        self,
        callback: Callable[[], Awaitable[Any]],
        *,
        force_execution: bool = False,
    ) -> Any:
        """Run ``callback`` under the cache / lock / lifecycle gate.

        Args:
            callback: Async callable that performs the job body.
            force_execution: Bypass completed-result and lifecycle checks for
                manual recovery. It never steals or bypasses an active lock.

        Returns:
            The callback return value (cached on success), the cached
            prior result, or ``None`` when the run was deliberately
            skipped (cooldown / lifecycle-already-ran).

        Raises:
            IdempotencyOverlapException: When a durable-intent job opted into
                redelivery and another callback owns the same lease.
        """
        self._idempotency_key = self.generate_idempotency_key()
        Log.debug(
            "Job idempotency key: %s", self._idempotency_key, category="idempotency"
        )

        job_force = getattr(self, "force", False)
        is_forced = force_execution or job_force
        bypass_result_cache = is_forced or bool(
            getattr(self, "bypass_idempotency_result", False)
        )

        if is_forced:
            Log.debug(
                "Recovery execution enabled - bypassing result/lifecycle checks",
                category="idempotency",
            )

        # Cross-process envelopes may explicitly bypass only the completed
        # result cache. That is distinct from operator recovery: lifecycle
        # policy still applies and the active owner lock is always respected.
        if not bypass_result_cache and getattr(self, "idempotency_cache_results", True):
            cache_key = self._result_key()
            if Cache.has(cache_key):
                cached_raw = Cache.get(cache_key)
                cached_result = None if cached_raw == self._NONE_SENTINEL else cached_raw
                Log.debug(
                    "Job already completed (cached): %s",
                    self.get_job_identifier(),
                    category="idempotency",
                )
                self._emit_cache_op_metric("get", "hit")
                self._emit_idempotency_metric("collision")
                return cached_result
            self._emit_cache_op_metric("get", "miss")

        if not is_forced and not self.should_execute_based_on_lifecycle():
            Log.debug(
                "Job skipped (already processed): %s",
                self.get_job_identifier(),
                category="idempotency",
            )
            self._emit_idempotency_metric("lifecycle_skip")
            return None

        # Recovery/manual re-runs may bypass stale completed-result evidence,
        # but never an active owner. Lock stealing allows two external-side-effect
        # jobs to run concurrently and lets the older worker delete the
        # newer worker's lock on exit.
        if self.is_job_locked():
            Log.debug(
                "Job already running; waiting: %s",
                self.get_job_identifier(),
                category="idempotency",
            )
            self._emit_idempotency_metric("locked")
            if getattr(self, "retry_on_idempotency_overlap", False):
                raise IdempotencyOverlapException(
                    f"Idempotency lease is active for {self.get_job_identifier()}"
                )
            return await self.wait_for_completion()

        self._emit_idempotency_metric("fresh")
        return await self._execute_with_lock(callback)

    # ── Key generation + parameter normalization ───────────────────

    def generate_idempotency_key(self) -> str:
        """Generate unique idempotency key from class + parameters."""
        job_data = {
            "job_class": self.__class__.__name__,
            "parameters": self.get_job_parameters(),
            "version": "1.0",
        }
        key_string = json.dumps(job_data, sort_keys=True)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def get_job_parameters(self) -> dict[str, Any]:
        """Return job parameters for idempotency key generation.

        Default behaviour: include every public, non-callable instance
        attribute (excluding queue-runner internals).

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
            if key.startswith("_"):
                continue
            if key in {"queue", "routing_key", "attempts", "job_id"}:
                continue
            if callable(value):
                continue
            if whitelist is not None and key not in whitelist:
                continue
            normalized = self._normalize_param_value(value)
            if normalized is not None:
                params[key] = normalized
        return params

    def _normalize_param_value(self, value: Any) -> Any:
        """Normalize parameter values into stable JSON-serializable shapes."""
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (list, tuple, set)):
            normalized_items = [self._normalize_param_value(v) for v in value]
            return [v for v in normalized_items if v is not None]
        if isinstance(value, dict):
            normalized_dict: dict[str, Any] = {}
            for key in sorted(value.keys(), key=lambda k: str(k)):
                nv = self._normalize_param_value(value[key])
                if nv is not None:
                    normalized_dict[str(key)] = nv
            return normalized_dict
        if hasattr(value, "id"):
            return value.id
        if hasattr(value, "public_id"):
            return value.public_id
        return None

    # ── Cache / lock primitives ────────────────────────────────────

    def _result_key(self) -> str:
        """Cache key for this job's result-cache entry."""
        return f"{self.RESULT_KEY_PREFIX}{self._idempotency_key}"

    def _lock_key(self) -> str:
        """Cache key for this job's exclusive lease."""
        return f"{self.LOCK_KEY_PREFIX}{self._idempotency_key}"

    def _fence_key(self) -> str:
        """Cache key for this job's monotonic owner-fence counter."""
        return f"{self.FENCE_KEY_PREFIX}{self._idempotency_key}"

    def get_cached_result(self) -> Any | None:
        """Read the cached result, decoding the None-sentinel.

        ``Cache.get`` returning ``None`` overlaps with "absent"
        and "cached None" on every driver. ``wrap_with_idempotency``
        uses ``Cache.has`` + a sentinel-aware decode for the canonical
        check; this accessor decodes correctly when callers ask but
        still returns ``None`` for "truly absent" so the existing
        ``if cached is not None`` shape in external callers remains
        meaningful.
        """
        cache_key = self._result_key()
        raw = Cache.get(cache_key)
        if raw == self._NONE_SENTINEL:
            return None
        return raw

    def cache_result(self, result: Any) -> None:
        """Cache a job's terminal result, encoding ``None`` to the
        sentinel so the next dispatch doesn't mistake "cached None"
        for "never ran".

        TTL is result-shaped: a real payload proves the work happened
        and dedupes for ``IDEMPOTENCY_CACHE_TTL``; a bare ``None`` is
        indistinguishable from a claim-miss no-op and only holds for
        ``IDEMPOTENCY_NONE_TTL`` (see the knob's doc for the poisoning
        story this prevents)."""
        cache_key = self._result_key()
        stored = self._NONE_SENTINEL if result is None else result
        ttl = self.IDEMPOTENCY_NONE_TTL if result is None else self.IDEMPOTENCY_CACHE_TTL
        Cache.put(cache_key, stored, ttl)

    def is_job_locked(self) -> bool:
        lock_key = self._lock_key()
        return Cache.has(lock_key)

    def acquire_job_lock(self) -> bool:
        """Acquire an owner-fenced exclusive lease atomically."""
        lock_key = self._lock_key()
        fence_key = self._fence_key()
        fence = Cache.increment(
            fence_key,
            1,
            max(self.IDEMPOTENCY_CACHE_TTL, self.JOB_LOCK_TTL) * 7,
        )
        lock_data = {
            "owner": uuid.uuid4().hex,
            "fence": int(fence),
            "started_at": pendulum.now("UTC").isoformat(),
            "job_class": self.__class__.__name__,
            "parameters": self.get_job_parameters(),
        }
        # The lock MUST outlive the job. With a flat 30m TTL, a job whose
        # own ``timeout`` is also ~30m (e.g. CleanOrphansJob) could have its
        # lock auto-expire at the moment it's still running — a second worker
        # then starts a duplicate, and this worker's ``finally`` release
        # deletes the SECOND worker's freshly-acquired lock, cascading into a
        # third. Size the TTL strictly above the job's enforced timeout so the
        # lock can never lapse mid-run.
        ttl = max(self.JOB_LOCK_TTL, int(getattr(self, "timeout", 0) or 0) + 300)
        acquired = Cache.add(lock_key, lock_data, ttl)
        if acquired:
            self._idempotency_lock_value = lock_data
            self._idempotency_fence = int(fence)
        return acquired

    def release_job_lock(self) -> None:
        """Release the idempotency lock.

        Cache failures are swallowed defensively. ``release_job_lock``
        is invoked from the ``finally`` block of ``_execute_with_lock``;
        an exception escaping here would REPLACE any in-flight
        callback exception (Python's exception-during-finally
        semantic), so a runtime Redis outage that fires mid-job would
        silently transmute a precise domain exception (e.g. a
        ``PermanentJobError`` with ``do_not_retry=True``) into a
        generic ``ConnectionError`` — and every upstream consumer
        keyed on the original class (the queue worker retry router's
        do_not_retry branch, per-class retry policies, per-error
        handlers) silently mishandles the failure. The same shape
        happens for successful jobs: the result is computed and
        cached, but the caller sees the cache-forget exception
        instead of the return value.

        This is a best-effort release: log on failure and never bubble. The
        lock leaks for at most
        ``JOB_LOCK_TTL`` (30m) on the unlikely path where Cache is
        still down when the TTL expires.
        """
        lock_key = self._lock_key()
        expected = self._idempotency_lock_value
        if expected is None:
            return
        try:
            Cache.forget_if(lock_key, expected)
        except Exception as exc:
            Log.warning(
                "Failed to release idempotency lock %s: %s",
                lock_key,
                exc,
                category="idempotency",
            )
        finally:
            self._idempotency_lock_value = None

    # ── Lifecycle / cooldown hooks (override in subclass) ──────────

    def get_lifecycle_step(self) -> str | None:
        """Return the app-defined lifecycle step name for this job, or
        ``None`` when no lifecycle gating applies. Subclass override."""
        return None

    def should_execute_based_on_lifecycle(self) -> bool:
        """Decide whether the job should run based on the app's
        lifecycle store. Default ``True`` — subclass override to
        consult an entity-keyed step log."""
        return True

    def should_collect_again(self) -> bool:
        """Decide whether a collection job (no entity id, source-driven)
        should run again given any cooldown the app enforces. Default
        ``True`` — subclass override for per-source cooldowns.

        A subclass that wants the standard per-source cooldown returns
        :meth:`_claim_source_cooldown` from here and configures the class
        attributes above it. The default stays a pass-through on purpose:
        a job that merely happens to carry a ``source`` attribute must not
        start claiming cooldown keys because it inherited this mixin.
        """
        return True

    def _claim_source_cooldown(self) -> bool:
        """Atomically claim this poll's cooldown window; ``True`` if it won.

        Poll-style jobs (feed sweeps, inventory refreshes, discovery runs)
        are dispatched by schedulers that can fire the same poll twice
        inside one window — two ticks racing, a redelivery, an operator
        re-run. Claiming a per-``(source, grains)`` key with SETNX + TTL
        lets exactly the first caller through; the key expires with the
        cooldown so the next window re-claims it. A ``get`` → ``put``
        check-then-act here does NOT work: both racing callers read
        "expired", both pass, and the upstream is polled twice.

        Three escape hatches, in order:

        * no ``source`` attribute — not a poll, never throttled;
        * a truthy ``force`` attribute — an operator asked for this run;
        * :attr:`cooldown_requires_grains` with no grain resolved — a
          source-only key is a GLOBAL claim across every entity, so fail
          open rather than let one job hold the whole fleet's key. The
          job's own durable gate (claim token, TTL, lifecycle row) is the
          real authority.
        """
        source = getattr(self, "source", None)
        if not hasattr(self, "source"):
            return True

        if getattr(self, "force", None):
            Log.debug(
                "Force flag enabled - bypassing cooldown for %s",
                source,
                category="idempotency",
            )
            return True

        cooldown_minutes = self.source_cooldown_minutes.get(
            source,
            int(
                config(
                    "jobs.source_cooldown_minutes",
                    self.default_source_cooldown_minutes,
                )
            ),
        )

        # Grains come from the same identity tuple the idempotency key is
        # built from, so the cooldown key and the idempotency key can never
        # disagree about what makes two dispatches "the same poll".
        grains = [
            str(getattr(self, attr))
            for attr in self.cooldown_grain_attrs
            if getattr(self, attr, None)
        ]
        if self.cooldown_requires_grains and not grains:
            return True

        time_key = self.COOLDOWN_KEY_PREFIX + ":".join([str(source), *grains])

        if Cache.add(time_key, pendulum.now("UTC").isoformat(), cooldown_minutes * 60):
            Log.debug(
                "Cooldown claim taken for %s (cooldown: %sm)",
                source,
                cooldown_minutes,
                category="idempotency",
            )
            return True

        Log.debug(
            "Cooldown active for %s (cooldown: %sm)",
            source,
            cooldown_minutes,
            category="idempotency",
        )
        return False

    # ── Metric emission hooks (override in subclass) ───────────────

    def _emit_idempotency_metric(self, outcome: str) -> None:
        """Fire a Prometheus counter for an idempotency outcome.

        Outcomes used by the base flow:
        ``"collision"``, ``"locked"``, ``"lifecycle_skip"``, ``"fresh"``.
        Subclass override to emit; the base class is intentionally
        a no-op so cara doesn't depend on app metric definitions.
        """

    def _emit_cache_op_metric(self, operation: str, outcome: str) -> None:
        """Fire a Prometheus counter for a cache operation. Subclass override."""

    # ── Execution + waiter ─────────────────────────────────────────

    async def _execute_with_lock(self, callback: Callable[[], Awaitable[Any]]) -> Any:
        """Acquire lock, run callback, cache result, release lock.

        Lock-acquisition failure waits for the owner by default. Durable
        intent jobs instead raise a throttle so their own callback is
        redelivered after the owner releases the lease.
        Previously: caller A raced caller B between
        ``is_job_locked()`` (returned False) and ``acquire_job_lock``
        (returned False because B already won). A then returned None
        silently — the caller (a controller, a listener) thought the
        idempotent work ran when it didn't, so no result was ever
        produced for this dispatch. Now A waits on B's completion
        and returns B's cached result.
        """
        if not self.acquire_job_lock():
            Log.debug(
                "Lock acquire lost race for %s; waiting on the in-flight run",
                self.get_job_identifier(),
                category="idempotency",
            )
            self._emit_idempotency_metric("locked")
            if getattr(self, "retry_on_idempotency_overlap", False):
                raise IdempotencyOverlapException(
                    f"Idempotency lease race for {self.get_job_identifier()}"
                )
            return await self.wait_for_completion()

        try:
            Log.debug(
                "Executing job with idempotency: %s",
                self.get_job_identifier(),
                category="idempotency",
            )
            result = await callback()
            if getattr(self, "idempotency_cache_results", True):
                self.cache_result(result)
            Log.debug(
                "Job completed successfully: %s",
                self.get_job_identifier(),
                category="idempotency",
            )
            return result
        except Exception as e:
            Log.error(
                "Job failed: %s - %s",
                self.get_job_identifier(),
                e,
                category="idempotency",
            )
            raise
        finally:
            self.release_job_lock()

    async def wait_for_completion(self) -> Any | None:
        """Wait for another worker's run to finish; return cached result.

        ``max_wait_time`` / ``check_interval`` are deliberately fixed —
        the lock TTL caps tail latency at ``JOB_LOCK_TTL``. Returns
        ``None`` on timeout or when the primary finished without
        caching (graceful early-return); downstream code already
        treats ``None`` as "retry next cycle".
        """
        max_wait_time = 300  # 5 minutes
        check_interval = 5
        waited = 0
        cache_key = self._result_key()

        while waited < max_wait_time:
            await asyncio.sleep(check_interval)
            waited += check_interval

            # Sentinel-aware existence check — see
            # ``wrap_with_idempotency`` for the rationale: a job that
            # returns ``None`` must be observable to waiters as
            # "completed", not "still running".
            if Cache.has(cache_key):
                cached_raw = Cache.get(cache_key)
                cached_result = None if cached_raw == self._NONE_SENTINEL else cached_raw
                Log.debug(
                    "Waited job completed: %s",
                    self.get_job_identifier(),
                    category="idempotency",
                )
                return cached_result

            if not self.is_job_locked():
                Log.debug(
                    "Job lock released without cached result: %s",
                    self.get_job_identifier(),
                    category="idempotency",
                )
                break

        Log.debug(
            "Timeout waiting for job: %s",
            self.get_job_identifier(),
            category="idempotency",
        )
        return None

    # ── Identity / debug ───────────────────────────────────────────

    def get_job_identifier(self) -> str:
        """Human-readable identifier used in log lines."""
        params = self.get_job_parameters()
        if "product_id" in params:
            return f"{self.__class__.__name__}(product_id={params['product_id']})"
        elif "source" in params:
            return f"{self.__class__.__name__}(source={params['source']})"
        return f"{self.__class__.__name__}({params})"


__all__ = ["MakesIdempotentBase"]
