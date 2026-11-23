"""A step's returned awaitable is awaited; each run reports only its steps.

Live defects in ``cara/workflows/Pipeline.py``:

1. The pipeline awaited a step only when its callable LOOKED async
   (``iscoroutinefunction``). A sync ``handle`` that returns a coroutine —
   a decorated or delegating handle — was recorded as a success while its
   coroutine never ran (and its exit code was never judged).
2. ``execute()`` appended to the ``results`` list a previous run had
   already returned, so a second run reported every step twice and
   rewrote the first run's report under its caller.
"""

from __future__ import annotations

import pytest

from cara.workflows import Pipeline, PipelineType

ran: list[str] = []


async def _work(name: str, code: int = 0) -> int:
    ran.append(name)
    return code


class _DelegatingHandle:
    def handle(self):
        return _work("delegating")


class _DelegatingFailure:
    def handle(self):
        return _work("failing", code=2)


class _Application:
    def call(self, target, **kwargs):
        return target(**kwargs)


class _ContainerStep:
    application = _Application()

    def handle(self, label: str = "container"):
        return _work(label)


class _CallableStep:
    def __call__(self):
        return _work("callable")


class _Ok:
    def handle(self) -> int:
        return 0


@pytest.fixture(autouse=True)
def _reset_ran():
    ran.clear()


@pytest.mark.asyncio
async def test_a_sync_handle_returning_a_coroutine_is_awaited() -> None:
    result = await Pipeline.create(PipelineType.SYNC).add(_DelegatingHandle).execute()

    assert ran == ["delegating"]
    assert result["success"] is True


@pytest.mark.asyncio
async def test_the_awaited_exit_code_is_judged() -> None:
    result = await Pipeline.create(PipelineType.SYNC).add(_DelegatingFailure).execute()

    assert ran == ["failing"]
    assert result["success"] is False
    assert result["results"][0]["success"] is False


@pytest.mark.asyncio
async def test_a_container_call_returning_a_coroutine_is_awaited() -> None:
    result = await (
        Pipeline.create(PipelineType.SYNC)
        .add(_ContainerStep, label="via-container")
        .execute()
    )

    assert ran == ["via-container"]
    assert result["success"] is True


@pytest.mark.asyncio
async def test_a_callable_step_returning_a_coroutine_is_awaited() -> None:
    result = await Pipeline.create(PipelineType.SYNC).add(_CallableStep).execute()

    assert ran == ["callable"]
    assert result["success"] is True


@pytest.mark.asyncio
async def test_each_execute_reports_only_its_own_steps() -> None:
    pipeline = Pipeline.create(PipelineType.SYNC).add(_Ok)

    first = await pipeline.execute()
    second = await pipeline.execute()

    assert [row["step"] for row in second["results"]] == ["_Ok"]
    assert [row["step"] for row in first["results"]] == ["_Ok"], (
        "the first run's report was rewritten by the second run"
    )
