"""A step returning a bool is not an exit code, and a raising ``on_success``
hook does not fail the step it follows.

``True`` is an int to Python, so a step returning ``True`` ("it worked")
failed as "exit code True"; and an ``on_success`` hook that raised ran
inside the step's ``try``, so the step was recorded twice — once
succeeded, once failed — and ``on_failure`` fired for a completed step.
"""

from __future__ import annotations

import pytest

from cara.workflows import Pipeline, PipelineType


class _ReturnsTrue:
    def handle(self) -> bool:
        return True


class _ReturnsFalse:
    def handle(self) -> bool:
        return False


class _ReturnsZero:
    def handle(self) -> int:
        return 0


@pytest.mark.asyncio
async def test_a_bool_result_is_not_an_exit_code() -> None:
    result = (
        await Pipeline.create(PipelineType.SYNC)
        .add(_ReturnsTrue)
        .add(_ReturnsFalse)
        .execute()
    )

    assert result["success"] is True
    assert [row["success"] for row in result["results"]] == [True, True]


@pytest.mark.asyncio
async def test_a_raising_success_hook_leaves_the_step_a_success() -> None:
    failures: list[dict] = []

    def hook(_row, _context) -> None:
        raise ValueError("hook failed")

    result = (
        await Pipeline.create(PipelineType.SYNC)
        .add(
            _ReturnsZero, on_success=hook, on_failure=lambda row, _c: failures.append(row)
        )
        .execute()
    )

    assert result["success"] is True
    assert result["successful_steps"] == 1
    assert len(result["results"]) == 1
    assert result["results"][0]["success"] is True
    assert result["results"][0]["on_success_error"] == "hook failed"
    assert failures == []
