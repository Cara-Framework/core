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
from collections.abc import Awaitable, Callable
from typing import Any

import pendulum

from cara.context import ExecutionContext
from cara.facades import Cache, Log


class MakesIdempotentBase:
    """Laravel-style trait for flow-level idempotency (opt-in per job).

    Subclasses define their own ``handle()`` and run the body through
    :meth:`wrap_with_idempotency` so cache / locks / lifecycle checks
    apply consistently.

    Features:
        * SHA-256 idempotency key derived from class name + job parameters
        * Result cache (``IDEMPOTENCY_CACHE_TTL`` seconds) for de-dup of
          completed runs
        * Distributed lock (``JOB_LOCK_TTL`` seconds) — concurrent
          duplicate dispatches converge on a single execution
        * Hooks for app-defined lifecycle gating + per-source cooldowns
        * Optional metric emission via subclass hooks
    """

    #: Cache TTL for result cache (24 hours).
    IDEMPOTENCY_CACHE_TTL = 24 * 60 * 60

    #: Lock TTL for active jobs (30 minutes).
    JOB_LOCK_TTL = 30 * 60

    #: Sentinel that encodes a cached ``None`` return value. Without
    #: this, ``cache_result(None)`` writes ``None`` and
    #: ``get_cached_result()`` can't tell "ran successfully and
    #: returned nothing" from "never ran" — every subsequent dispatch
    #: of a None-returning job (every fire-and-forget pipeline step)
    #: re-executed the work it was supposed to skip. Stored as a
    #: stable string literal so JSON / pickle Cache drivers
    #: round-trip it identically.
    _NONE_SENTINEL = "__cara_idempotent_none__"

    #: Keep idempotency semantics identical between queue workers and
    #: sync paths. Legacy jobs can opt out by setting
    #: ``enforce_sync_idempotency = False``.
    enforce_sync_idempotency = True

    #: Whether this job participates in the 24h *result cache* dedup.
    #: Per-entity pipeline jobs keep this True (re-dispatching the same
    #: listing_id must not re-run the work). RECURRING SCHEDULED jobs set
    #: it False — they hash to one stable key (no per-run params), so the
    #: result cache would dedupe every tick after the first into a single
    #: run per IDEMPOTENCY_CACHE_TTL (24h). The scheduler flips this off
    #: for scheduled invocations. Overlap is still guarded by the job lock
    #: (and any WithoutOverlapping / cross-process lock the job declares).
    idempotency_cache_results = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._idempotency_key: str | None = None

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
            force_execution: Bypass every idempotency check (for
                manual recovery / one-off retries).

        Returns:
            The callback return value (cached on success), the cached
            prior result, or ``None`` when the run was deliberately
            skipped (cooldown / lifecycle-already-ran).
        """
        if ExecutionContext.is_sync() and not getattr(
            self, "enforce_sync_idempotency", True
        ):
            Log.debug("Sync mode idempotency bypass enabled", category="idempotency")
            return await callback()

        self._idempotency_key = self.generate_idempotency_key()
        Log.debug("Job idempotency key: %s", self._idempotency_key, category='idempotency')

        job_force = getattr(self, "force", False)
        is_forced = force_execution or job_force

        if is_forced:
            Log.debug(
                "Force execution enabled - bypassing idempotency checks",
                category="idempotency",
            )

        if not is_forced:
            # Recurring scheduled jobs opt out of the result cache (see
            # ``idempotency_cache_results``) — they MUST run every tick.
            # The lock + lifecycle checks below still apply, so overlapping
            # ticks are still serialised.
            if getattr(self, "idempotency_cache_results", True):
                cache_key = f"job_result:{self._idempotency_key}"
                # Read the raw cache value once so we can distinguish
                # "cached None" (sentinel) from "no entry" (truly missing).
                # ``Cache.has`` is the authoritative miss/hit signal —
                # ``Cache.get`` returning ``None`` overlaps with the
                # "absent" case on every driver.
                if Cache.has(cache_key):
                    cached_raw = Cache.get(cache_key)
                    cached_result = (
                        None if cached_raw == self._NONE_SENTINEL else cached_raw
                    )
                    Log.debug("Job already completed (cached): %s", self.get_job_identifier(), category='idempotency')
                    self._emit_cache_op_metric("get", "hit")
                    self._emit_idempotency_metric("collision")
                    return cached_result
                self._emit_cache_op_metric("get", "miss")

            if self.is_job_locked():
                Log.debug("Job already running, skipping: %s", self.get_job_identifier(), category='idempotency')
                self._emit_idempotency_metric("locked")
                return None

            if not self.should_execute_based_on_lifecycle():
                Log.debug("Job skipped (already processed): %s", self.get_job_identifier(), category='idempotency')
                self._emit_idempotency_metric("lifecycle_skip")
                return None

        self._emit_idempotency_metric("fresh")
        return await self._execute_with_lock(callback, force_lock=is_forced)

    async def idempotent_execute(
        self,
        callback: Callable[[], Awaitable[Any]],
        *,
        force_execution: bool = False,
    ) -> Any:
        """Alias for :meth:`wrap_with_idempotency`."""
        return await self.wrap_with_idempotency(callback, force_execution=force_execution)

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
        cache_key = f"job_result:{self._idempotency_key}"
        raw = Cache.get(cache_key)
        if raw == self._NONE_SENTINEL:
            return None
        return raw

    def cache_result(self, result: Any) -> None:
        """Cache a job's terminal result, encoding ``None`` to the
        sentinel so the next dispatch doesn't mistake "cached None"
        for "never ran"."""
        cache_key = f"job_result:{self._idempotency_key}"
        stored = self._NONE_SENTINEL if result is None else result
        Cache.put(cache_key, stored, self.IDEMPOTENCY_CACHE_TTL)

    def is_job_locked(self) -> bool:
        lock_key = f"job_lock:{self._idempotency_key}"
        return Cache.has(lock_key)

    def acquire_job_lock(self) -> bool:
        """Acquire exclusive lock atomically (Cache.add only-if-absent)."""
        lock_key = f"job_lock:{self._idempotency_key}"
        lock_data = {
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
        return Cache.add(lock_key, lock_data, ttl)

    def release_job_lock(self) -> None:
        """Release the idempotency lock.

        Cache failures are swallowed defensively. ``release_job_lock``
        is invoked from the ``finally`` block of ``_execute_with_lock``;
        an exception escaping here would REPLACE any in-flight
        callback exception (Python's exception-during-finally
        semantic), so a runtime Redis outage that fires mid-job would
        silently transmute a precise domain exception (e.g.
        ``PermanentScrapeError`` with ``do_not_retry=True``) into a
        generic ``ConnectionError`` — and every upstream consumer
        keyed on the original class (``AMQPDriver._handle_failed_message``
        do_not_retry branch, per-class retry policies, per-error
        handlers) silently mishandles the failure. The same shape
        happens for successful jobs: the result is computed and
        cached, but the caller sees the cache-forget exception
        instead of the return value.

        Mirrors the existing defensive shape on
        ``UniqueJob.release_unique_lock`` — best-effort release, log
        on failure, never bubble. The lock leaks for at most
        ``JOB_LOCK_TTL`` (30m) on the unlikely path where Cache is
        still down when the TTL expires.
        """
        lock_key = f"job_lock:{self._idempotency_key}"
        try:
            Cache.forget(lock_key)
        except Exception as exc:
            Log.warning("Failed to release idempotency lock %s: %s", lock_key, exc, category='idempotency')

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
        ``True`` — subclass override for per-source cooldowns."""
        return True

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

    async def _execute_with_lock(
        self, callback: Callable[[], Awaitable[Any]], *, force_lock: bool = False
    ) -> Any:
        """Acquire lock, run callback, cache result, release lock.

        Lock-acquisition failure now falls through to
        ``wait_for_completion`` instead of returning ``None``.
        Previously: caller A raced caller B between
        ``is_job_locked()`` (returned False) and ``acquire_job_lock``
        (returned False because B already won). A then returned None
        silently — the caller (a controller, a listener) thought the
        idempotent work ran when it didn't, so no result was ever
        produced for this dispatch. Now A waits on B's completion
        and returns B's cached result.
        """
        if not self.acquire_job_lock():
            if force_lock:
                self.release_job_lock()
                self.acquire_job_lock()
                Log.debug("Force-acquired lock for %s", self.get_job_identifier(), category='idempotency')
            else:
                Log.debug("Lock acquire lost race for %s; waiting on the in-flight run", self.get_job_identifier(), category='idempotency')
                self._emit_idempotency_metric("locked")
                return await self.wait_for_completion()

        try:
            Log.debug("Executing job with idempotency: %s", self.get_job_identifier(), category='idempotency')
            result = await callback()
            if getattr(self, "idempotency_cache_results", True):
                self.cache_result(result)
            Log.debug("Job completed successfully: %s", self.get_job_identifier(), category='idempotency')
            return result
        except Exception as e:
            Log.error("Job failed: %s - %s", self.get_job_identifier(), e, category='idempotency')
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
        cache_key = f"job_result:{self._idempotency_key}"

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
                Log.debug("Waited job completed: %s", self.get_job_identifier(), category='idempotency')
                return cached_result

            if not self.is_job_locked():
                Log.debug("Job lock released without cached result: %s", self.get_job_identifier(), category='idempotency')
                break

        Log.debug("Timeout waiting for job: %s", self.get_job_identifier(), category='idempotency')
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
