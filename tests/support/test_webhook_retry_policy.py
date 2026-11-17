"""A webhook receiver's non-2xx: only 410 ends the delivery; Retry-After is
honoured within a ceiling."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from cara.support import (
    WEBHOOK_RETRY_AFTER_CEILING_SECONDS,
    webhook_answer_is_terminal,
    webhook_retry_after,
)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 408, 409, 425, 429, 500, 503])
def test_every_answer_but_gone_is_retried(status) -> None:
    assert webhook_answer_is_terminal(status) is False


def test_gone_is_terminal() -> None:
    assert webhook_answer_is_terminal(410) is True


def test_retry_after_seconds_and_dates_within_the_ceiling() -> None:
    now = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
    assert webhook_retry_after({"Retry-After": "120"}) == 120
    assert webhook_retry_after({"retry-after": "1.2"}) == 2
    assert (
        webhook_retry_after({"Retry-After": "Tue, 17 Nov 2026 12:05:00 GMT"}, now=now)
        == 300
    )
    assert webhook_retry_after({"Retry-After": "999999"}) == (
        WEBHOOK_RETRY_AFTER_CEILING_SECONDS
    )
    assert webhook_retry_after({"Retry-After": "soon"}) is None
    assert webhook_retry_after({}) is None
