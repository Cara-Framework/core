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

import asyncio
import hashlib
import json
from typing import Any, Awaitable, Callable, Dict, Optional

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

    #: Keep idempotency semantics identical between queue workers and
    #: sync paths. Legacy jobs can opt out by setting
    #: ``enforce_sync_idempotency = False``.
    enforce_sync_idempotency = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._idempotency_key: Optional[str] = None

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
        if (
            ExecutionContext.is_sync()
            and not getattr(self, "enforce_sync_idempotency", True)
        ):
            Log.debug("Sync mode idempotency bypass enabled", category="idempotency")
            return await callback()

        self._idempotency_key = self.generate_idempotency_key()
        Log.debug(
            f"Job idempotency key: {self._idempotency_key}", category="idempotency"
        )

        job_force = getattr(self, "force", False)
        is_forced = force_execution or job_force

        if is_forced:
            Log.debug(
                "Force execution enabled - bypassing idempotency checks",
                category="idempotency",
            )

        if not is_forced:
            cached_result = self.get_cached_result()
            if cached_result is not None:
                Log.debug(
                    f"Job already completed (cached): {self.get_job_identifier()}",
                    category="idempotency",
                )
                self._emit_cache_op_metric("get", "hit")
                self._emit_idempotency_metric("collision")
                return cached_result
            self._emit_cache_op_metric("get", "miss")

            if self.is_job_locked():
                Log.debug(
                    f"Job already running, skipping: {self.get_job_identifier()}",
                    category="idempotency",
                )
                self._emit_idempotency_metric("locked")
                return None

            if not self.should_execute_based_on_lifecycle():
                Log.debug(
                    f"Job skipped (already processed): {self.get_job_identifier()}",
                    category="idempotency",
                )
                self._emit_idempotency_metric("lifecycle_skip")
                return None

        self._emit_idempotency_metric("fresh")
        return await self._execute_with_lock(callback)

    async def idempotent_execute(
        self,
        callback: Callable[[], Awaitable[Any]],
        *,
        force_execution: bool = False,
    ) -> Any:
        """Alias for :meth:`wrap_with_idempotency`."""
        return await self.wrap_with_idempotency(
            callback, force_execution=force_execution
        )

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

    def get_job_parameters(self) -> Dict[str, Any]:
        """Return job parameters for idempotency key generation.

        Default behaviour: include every public, non-callable instance
        attribute (excluding queue-runner internals). Subclasses
        commonly override to pin a smaller / different set.
        """
        params: Dict[str, Any] = {}
        for key, value in vars(self).items():
            if key.startswith("_"):
                continue
            if key in {"queue", "routing_key", "attempts", "job_id"}:
                continue
            if callable(value):
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
            normalized_dict: Dict[str, Any] = {}
            for key in sorted(value.keys(), key=lambda k: str(k)):
                nv = self._normalize_param_value(value[key])
                if nv is not None:
                    normalized_dict[str(key)] = nv
            return normalized_dict
        if hasattr(value, "id"):
            return getattr(value, "id")
        if hasattr(value, "public_id"):
            return getattr(value, "public_id")
        return None

    # ── Cache / lock primitives ────────────────────────────────────

    def get_cached_result(self) -> Optional[Any]:
        cache_key = f"job_result:{self._idempotency_key}"
        return Cache.get(cache_key)

    def cache_result(self, result: Any) -> None:
        cache_key = f"job_result:{self._idempotency_key}"
        Cache.put(cache_key, result, self.IDEMPOTENCY_CACHE_TTL)

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
        return Cache.add(lock_key, lock_data, self.JOB_LOCK_TTL)

    def release_job_lock(self) -> None:
        lock_key = f"job_lock:{self._idempotency_key}"
        Cache.forget(lock_key)

    # ── Lifecycle / cooldown hooks (override in subclass) ──────────

    def get_lifecycle_step(self) -> Optional[str]:
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
        self, callback: Callable[[], Awaitable[Any]]
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
            Log.debug(
                f"Lock acquire lost race for {self.get_job_identifier()}; "
                f"waiting on the in-flight run",
                category="idempotency",
            )
            self._emit_idempotency_metric("locked")
            return await self.wait_for_completion()

        try:
            Log.debug(
                f"Executing job with idempotency: {self.get_job_identifier()}",
                category="idempotency",
            )
            result = await callback()
            self.cache_result(result)
            Log.debug(
                f"Job completed successfully: {self.get_job_identifier()}",
                category="idempotency",
            )
            return result
        except Exception as e:
            Log.error(
                f"Job failed: {self.get_job_identifier()} - {e}",
                category="idempotency",
            )
            raise
        finally:
            self.release_job_lock()

    async def wait_for_completion(self) -> Optional[Any]:
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

        while waited < max_wait_time:
            await asyncio.sleep(check_interval)
            waited += check_interval

            cached_result = self.get_cached_result()
            if cached_result is not None:
                Log.debug(
                    f"Waited job completed: {self.get_job_identifier()}",
                    category="idempotency",
                )
                return cached_result

            if not self.is_job_locked():
                Log.debug(
                    f"Job lock released without cached result: {self.get_job_identifier()}",
                    category="idempotency",
                )
                break

        Log.debug(
            f"Timeout waiting for job: {self.get_job_identifier()}",
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
