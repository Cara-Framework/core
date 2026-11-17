"""Run blocking dependency probes concurrently, bounded and single-flight.

A readiness endpoint is hit by every load balancer on a fixed cadence. Its
probes are synchronous network round-trips (a ``SELECT 1``, a Redis PING), so
three things go wrong under a slow dependency:

* run one after another in one thread, the budgets ADD (four 1 s probes are a
  4 s readiness answer, past most LB timeouts);
* the budget ends the WAIT, never the probe — a socket call blocked in a
  worker thread cannot be cancelled;
* so every LB hit while the database is slow starts ANOTHER blocked probe
  behind the first: self-inflicted pool and thread exhaustion exactly when the
  database can least afford it.

:meth:`BoundedProbes.run` starts every probe at once in worker threads, waits
at most ``budget_ms`` in total, reports a probe still running as
``{"status": "timeout"}``, and never starts a second copy of a probe whose
previous run has not finished — the next call reports ``timeout`` for it
straight away until the stalled one returns.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any, ClassVar

from cara.context import ExecutionContext

TIMEOUT_STATUS = "timeout"


class BoundedProbes:
    """Concurrent, budgeted, single-flight probe runner (process-wide)."""

    _IN_FLIGHT: ClassVar[dict[str, asyncio.Future]] = {}

    @classmethod
    async def run(
        cls,
        probes: Mapping[str, Callable[[], dict[str, Any]]],
        *,
        budget_ms: int,
        namespace: str = "default",
    ) -> dict[str, dict[str, Any]]:
        """``{name: probe()}`` within ``budget_ms``; a late probe is ``timeout``.

        ``namespace`` keeps two endpoints' probes of the same name apart.
        A probe that raises reports ``{"status": "down", "error": <type>}``.
        """
        loop = asyncio.get_running_loop()
        running: dict[str, asyncio.Future] = {}
        skipped: set[str] = set()
        for name, probe in probes.items():
            key = f"{namespace}:{name}"
            previous = cls._IN_FLIGHT.get(key)
            if (
                previous is not None
                and not previous.done()
                and previous.get_loop() is loop
            ):
                skipped.add(name)
                continue
            future = asyncio.ensure_future(ExecutionContext.run_in_thread(probe))
            cls._IN_FLIGHT[key] = future
            future.add_done_callback(lambda f, k=key: cls._forget(k, f))
            running[name] = future

        if running:
            # ``asyncio.wait`` never cancels what it waits on: on timeout the
            # probes keep running in their threads (single-flight above).
            await asyncio.wait(list(running.values()), timeout=budget_ms / 1000)
        results: dict[str, dict[str, Any]] = {}
        for name in probes:
            future = running.get(name)
            if name in skipped or future is None or not future.done():
                results[name] = {"status": TIMEOUT_STATUS, "latency_ms": budget_ms}
            elif future.exception() is not None:
                results[name] = {
                    "status": "down",
                    "error": type(future.exception()).__name__,
                }
            else:
                results[name] = future.result()
        return results

    @classmethod
    def _forget(cls, key: str, future: asyncio.Future) -> None:
        if cls._IN_FLIGHT.get(key) is future:
            del cls._IN_FLIGHT[key]
        if not future.cancelled():
            future.exception()  # retrieved: a late failure is not "never retrieved"

    @classmethod
    def in_flight(cls, name: str, *, namespace: str = "default") -> bool:
        """Whether a previous run of ``name`` is still blocked in its thread."""
        future = cls._IN_FLIGHT.get(f"{namespace}:{name}")
        return future is not None and not future.done()
