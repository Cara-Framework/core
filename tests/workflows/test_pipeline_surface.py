"""The workflow pipeline's supported surface, pinned.

Two things are asserted here.

1. The conditional / context / callback features are real, working framework
   behaviour and must keep working even while no product happens to use them.
2. Queue-backed chain and batch orchestration is NOT part of the surface.
   It used to be advertised as ``PipelineType.ASYNC_CHAIN`` /
   ``ASYNC_PARALLEL`` plus a ``dispatch()`` method that raised on every call —
   a capability the framework does not have. A framework must not advertise
   what it cannot do, so the enum members and the dispatch path are gone and
   this test keeps them from creeping back without an implementation.
"""

from __future__ import annotations

import pytest

from cara.workflows.Pipeline import ConditionalPipeline, Pipeline, PipelineType


class _Ok:
    def handle(self) -> int:
        return 0


class _Boom:
    def handle(self) -> int:
        raise RuntimeError("boom")


class _Skipped:
    def handle(self) -> int:  # pragma: no cover - must never run
        raise AssertionError("condition-gated step ran anyway")


# ── supported surface ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_condition_skips_step_without_failing_the_pipeline():
    result = await (
        Pipeline.create(PipelineType.SYNC)
        .add(_Ok)
        .add(_Skipped, condition=lambda ctx: False)
        .execute()
    )

    assert result["success"] is True
    assert result["successful_steps"] == 1
    assert result["skipped_steps"] == 1
    assert result["total_steps"] == 2
    # A skipped step produces no result row — it was never attempted.
    assert [r["step"] for r in result["results"]] == ["_Ok"]


@pytest.mark.asyncio
async def test_condition_reads_pipeline_context():
    seen: list[dict] = []

    pipeline = Pipeline.create(PipelineType.SYNC).set_context("enabled", True)
    assert pipeline.get_context("enabled") is True
    assert pipeline.get_context("missing", "fallback") == "fallback"

    def condition(ctx: dict) -> bool:
        seen.append(dict(ctx))
        return bool(ctx.get("enabled"))

    result = await pipeline.add(_Ok, condition=condition).execute()

    assert seen == [{"enabled": True}]
    assert result["success"] is True
    assert result["skipped_steps"] == 0
    assert result["context"] == {"enabled": True}


@pytest.mark.asyncio
async def test_when_gates_every_step_it_adds():
    pipeline = Pipeline.create(PipelineType.SYNC)
    conditional = pipeline.when(lambda ctx: ctx.get("run") is True)
    assert isinstance(conditional, ConditionalPipeline)

    # ``when(...).add(...)`` returns the parent pipeline so the chain continues.
    assert conditional.add(_Skipped) is pipeline

    result = await pipeline.execute()

    assert result["success"] is True
    assert result["skipped_steps"] == 1
    assert result["successful_steps"] == 0


@pytest.mark.asyncio
async def test_success_and_failure_callbacks_receive_result_and_context():
    ok_calls: list[tuple] = []
    fail_calls: list[tuple] = []

    result = await (
        Pipeline.create(PipelineType.SYNC)
        .set_context("run_id", "abc")
        .add(_Ok, on_success=lambda res, ctx: ok_calls.append((res, ctx)))
        .add(_Boom, on_failure=lambda res, ctx: fail_calls.append((res, ctx)))
        .execute()
    )

    assert result["success"] is False
    assert len(ok_calls) == 1
    assert ok_calls[0][0]["step"] == "_Ok"
    assert ok_calls[0][1] == {"run_id": "abc"}

    assert len(fail_calls) == 1
    assert fail_calls[0][0]["success"] is False
    assert "boom" in fail_calls[0][0]["error"]


@pytest.mark.asyncio
async def test_every_step_skipped_is_still_a_successful_run():
    result = await (
        Pipeline.create(PipelineType.SYNC)
        .add(_Skipped, condition=lambda ctx: False)
        .execute()
    )

    assert result["success"] is True
    assert result["success_rate"] == 1.0
    assert result["successful_steps"] == 0
    assert result["skipped_steps"] == 1


# ── surface the framework must NOT advertise ────────────────────────


def test_only_sync_execution_is_advertised():
    assert [member.name for member in PipelineType] == ["SYNC"]


def test_no_queue_dispatch_surface():
    # ``dispatch()`` existed only to raise; queue chain/batch orchestration
    # needs durable signed descriptors before it can be offered again.
    for banned in ("dispatch", "_dispatch_chain", "_dispatch_parallel"):
        assert not hasattr(Pipeline, banned), f"Pipeline.{banned} is back"


def test_steps_carry_no_queue_routing_convention():
    # Routing keys were auto-derived from the step class name
    # ("<domain>.<priority>") — a product queue-naming convention that has no
    # business inside the framework's step model.
    pipeline = Pipeline.create(PipelineType.SYNC).add(_Ok)
    step = pipeline.steps[0]

    assert not hasattr(step, "routing_key")
    assert not hasattr(step, "priority")
