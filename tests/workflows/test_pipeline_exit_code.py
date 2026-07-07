"""A sync pipeline step that RETURNS a non-zero craft exit code must be
counted as a failed step — not silently reported as completed.

Regression: a seed command returning 1 (its snapshot import failed) was
counted as success, so ``setup:all`` printed "8/8 completed" while a step
had actually failed. Craft commands signal failure by return code, not by
raising, so the pipeline must honour the exit-code convention.
"""

from __future__ import annotations

import pytest

from cara.workflows import Pipeline, PipelineType


class _StepReturnsZero:
    def handle(self) -> int:
        return 0


class _StepReturnsOne:
    def handle(self) -> int:
        return 1


class _StepReturnsDict:
    # Non-int results are not exit codes — a step legitimately returning a
    # payload must stay successful.
    def handle(self) -> dict:
        return {"rows": 42}


class _StepRaises:
    def handle(self) -> int:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_zero_exit_code_is_success():
    result = await Pipeline.create(PipelineType.SYNC).add(_StepReturnsZero).execute()
    assert result["success"] is True
    assert result["successful_steps"] == 1


@pytest.mark.asyncio
async def test_nonzero_exit_code_is_failure():
    result = await Pipeline.create(PipelineType.SYNC).add(_StepReturnsOne).execute()
    assert result["success"] is False
    assert result["successful_steps"] == 0
    assert result["results"][0]["success"] is False


@pytest.mark.asyncio
async def test_non_int_result_stays_success():
    result = await Pipeline.create(PipelineType.SYNC).add(_StepReturnsDict).execute()
    assert result["success"] is True
    assert result["successful_steps"] == 1


@pytest.mark.asyncio
async def test_raising_step_is_failure():
    result = await Pipeline.create(PipelineType.SYNC).add(_StepRaises).execute()
    assert result["success"] is False
    assert result["successful_steps"] == 0


@pytest.mark.asyncio
async def test_mixed_steps_partial_success():
    result = await (
        Pipeline.create(PipelineType.SYNC)
        .add(_StepReturnsZero)
        .add(_StepReturnsOne)
        .add(_StepReturnsZero)
        .execute()
    )
    assert result["success"] is False
    assert result["successful_steps"] == 2
    assert result["total_steps"] == 3
