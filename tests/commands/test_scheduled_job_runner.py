"""A hand-run schedule entry runs exactly like a scheduler tick.

``maintenance:reconcile`` used to call ``handle()`` directly: no middleware
(so no overlap lock — a manual run could overlap the scheduled tick), no
entry kwargs for ``handle()``, and a broad ``suppress(Exception)``.
"""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

runner = importlib.import_module("cara.commands.core.ScheduledJobRunner")


class _App:
    def __init__(self) -> None:
        self.made: list[Any] = []

    def make(self, key: Any) -> Any:
        self.made.append(key)
        # Annotations are strings under ``from __future__ import annotations``.
        if key in (_Dependency, "_Dependency"):
            return _Dependency()
        if isinstance(key, type):
            return key()
        raise RuntimeError("unbound")


class _Dependency:
    pass


class _CentralJob:
    central_job = True

    def __init__(self, source: str = "default") -> None:
        self.source = source
        self.idempotency_cache_results = True
        self.calls: list[dict[str, Any]] = []

    async def handle(self, dep: _Dependency, limit: int = 1) -> None:
        self.calls.append({"dep": dep, "limit": limit, "source": self.source})


@pytest.fixture
def chain(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    seen: list[Any] = []

    async def _through_middleware(job: Any, handler: Any) -> Any:
        seen.append(job)
        return await handler(job)

    monkeypatch.setattr(runner, "run_through_middleware_async", _through_middleware)
    return seen


def test_entry_kwargs_reach_init_and_handle_through_the_middleware(chain) -> None:
    class _Job(_CentralJob):
        def __init__(self, source: str = "default", limit: int = 0) -> None:
            super().__init__(source)

    app = _App()
    assert asyncio.run(
        runner.run_scheduled_job_inline(app, _Job, {"source": "ebay", "limit": 7})
    )
    (job,) = chain
    assert job.source == "ebay"
    assert job.idempotency_cache_results is False
    assert job.calls[0]["limit"] == 7
    assert isinstance(job.calls[0]["dep"], _Dependency)


def test_init_type_error_falls_back_to_the_container(chain) -> None:
    app = _App()
    # ``limit`` is not an __init__ parameter: TypeError → container make.
    asyncio.run(runner.run_scheduled_job_inline(app, _CentralJob, {"limit": 3}))
    (job,) = chain
    assert job.source == "default"
    assert job.calls[0]["limit"] == 3
    assert _CentralJob in app.made


def test_a_job_without_handle_reports_false(chain) -> None:
    class _NoHandle:
        central_job = True

    assert asyncio.run(runner.run_scheduled_job_inline(_App(), _NoHandle)) is False
    assert chain == []


def test_the_real_middleware_requires_and_gets_a_central_scope() -> None:
    # No monkeypatch: the framework chain refuses a central job outside
    # ``Tenancy.central()``, so this proves the runner arms the scope.
    app = _App()
    assert asyncio.run(runner.run_scheduled_job_inline(app, _CentralJob)) is True


def test_handle_errors_propagate(chain) -> None:
    class _Boom(_CentralJob):
        async def handle(self) -> None:
            raise ValueError("boom")

    with pytest.raises(ValueError):
        asyncio.run(runner.run_scheduled_job_inline(_App(), _Boom))
