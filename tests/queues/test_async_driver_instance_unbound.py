"""
BUG-5: AsyncDriver._execute_job referenced `instance` in the except
block, but if instantiate_job() raised before assigning the variable
the handler would crash with NameError — masking the real error.

Fix: initialise ``instance = None`` before the try block so hasattr()
sees None and skips the failed() call gracefully.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest


def _make_driver():
    from cara.queues.drivers.AsyncDriver import AsyncDriver

    app = MagicMock()
    app.call = None  # no container-call path
    del app.call
    return AsyncDriver(application=app, options={})


class TestAsyncDriverInstanceUnbound:
    """Verify that a failing instantiate_job does NOT produce NameError."""

    def test_instantiation_failure_propagates_original_error(self):
        """Before the fix, this raised NameError instead of RuntimeError."""
        driver = _make_driver()

        with patch(
            "cara.queues.drivers.AsyncDriver.instantiate_job",
            side_effect=RuntimeError("bad job class"),
        ):
            with pytest.raises(RuntimeError, match="bad job class"):
                driver._execute_job(
                    job=MagicMock(),
                    options={"callback": "handle", "args": ()},
                    job_id=str(uuid.uuid4()),
                )

    def test_successful_job_still_works(self):
        """Sanity: the fix doesn't break the happy path."""
        driver = _make_driver()
        sentinel = object()

        mock_instance = MagicMock()
        mock_instance.handle = MagicMock(return_value=sentinel)

        with patch(
            "cara.queues.drivers.AsyncDriver.instantiate_job",
            return_value=mock_instance,
        ):
            # Should not raise
            driver._execute_job(
                job=MagicMock(),
                options={"callback": "handle", "args": ()},
                job_id=str(uuid.uuid4()),
            )
            mock_instance.handle.assert_called_once()
