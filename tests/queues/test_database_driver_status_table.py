"""
BUG-6: DatabaseDriver._update_job_status called self._get_builder({})
which resolved table=None (empty dict has no 'table' key).  Every job
status update silently failed inside the blanket except-Exception,
leaving started_at=NULL and breaking the stale-processing reaper.

Fix: pass self.options instead of {} so the builder picks up the
configured queue table and connection.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


def _make_driver(table="jobs", connection="pgsql"):
    from cara.queues.drivers.DatabaseDriver import DatabaseDriver

    app = MagicMock()
    opts = {"table": table, "connection": connection}
    return DatabaseDriver(application=app, options=opts), app


class TestDatabaseDriverStatusTable:
    """Verify _update_job_status uses the configured table, not None."""

    def test_status_update_uses_configured_table(self):
        """Before the fix, _get_builder received {} → table=None."""
        driver, app = _make_driver(table="queue_jobs", connection="pgsql")

        mock_builder = MagicMock()
        mock_builder.where.return_value = mock_builder
        mock_builder.first.return_value = None  # no existing row

        with patch.object(driver, "_get_builder", return_value=mock_builder) as gb:
            driver._update_job_status("job-123", "processing", {"started_at": "2025-01-01"})

            # _get_builder must receive self.options, not {}
            called_opts = gb.call_args[0][0]
            assert called_opts.get("table") == "queue_jobs", (
                f"Expected table='queue_jobs', got {called_opts}"
            )
            assert called_opts.get("connection") == "pgsql", (
                f"Expected connection='pgsql', got {called_opts}"
            )

    def test_status_update_does_not_pass_empty_dict(self):
        """Regression guard: empty dict must never reach _get_builder."""
        driver, app = _make_driver()

        mock_builder = MagicMock()
        mock_builder.where.return_value = mock_builder
        mock_builder.first.return_value = None

        with patch.object(driver, "_get_builder", return_value=mock_builder) as gb:
            driver._update_job_status("job-456", "completed", {"completed_at": "2025-01-01"})

            for c in gb.call_args_list:
                opts_arg = c[0][0]
                assert opts_arg != {}, (
                    "_get_builder was called with empty dict — table would be None"
                )
