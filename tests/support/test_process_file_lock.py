"""ProcessFileLock exposes one exact cross-platform serialization contract."""

from __future__ import annotations

import pytest

from cara.support.ProcessFileLock import ProcessFileLock


def test_second_owner_times_out_while_lock_is_held(tmp_path) -> None:
    path = tmp_path / "authority.lock"

    with (
        ProcessFileLock(path),
        pytest.raises(TimeoutError, match="authority.lock"),
        ProcessFileLock(path, timeout_seconds=0.05, poll_seconds=0.01),
    ):
        raise AssertionError("contended lock must not be entered")


@pytest.mark.parametrize("timeout", [0, -1, True, "1"])
def test_timeout_rejects_coercive_values(tmp_path, timeout) -> None:
    with pytest.raises(ValueError, match="timeout_seconds"):
        ProcessFileLock(tmp_path / "authority.lock", timeout_seconds=timeout)


@pytest.mark.parametrize("poll", [0, -1, True, "0.1"])
def test_poll_interval_rejects_coercive_values(tmp_path, poll) -> None:
    with pytest.raises(ValueError, match="poll_seconds"):
        ProcessFileLock(tmp_path / "authority.lock", poll_seconds=poll)
