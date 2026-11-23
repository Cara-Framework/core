"""``queue:purge`` exits 1 when tracker rows it was asked to delete failed.

Live defect: a per-row tracker delete failure was logged and the sweep went
on, then the command returned 0 whatever had failed. The per-row errors
scroll away; the exit status is what a deploy script or a cron reads. Before
the fix (``git show HEAD:cara/commands/core/QueuePurgeCommand.py``) the second
test returned 0.
"""

from __future__ import annotations

from cara.commands.core.QueuePurgeCommand import QueuePurgeCommand


class _Store:
    def __init__(self, *, undeletable: set[int] = frozenset()) -> None:
        self.undeletable = set(undeletable)
        self.deleted: list[int] = []

    def list_failed_jobs_older_than(self, *, cutoff, queue=None):
        return [{"id": 1, "queue": "sync"}, {"id": 2, "queue": "sync"}]

    def count_dead_lettered_older_than(self, *, cutoff, queue=None):
        return 0

    def delete_terminal_deliveries_older_than(self, *, cutoff, queue=None):
        return 0

    def delete_job(self, job_id):
        if job_id in self.undeletable:
            raise RuntimeError("violates foreign key constraint")
        self.deleted.append(job_id)
        return 1


def _purge(store: _Store) -> tuple[int, list[str]]:
    command = QueuePurgeCommand.__new__(QueuePurgeCommand)
    lines: list[str] = []
    command.line = lambda text="", *_a, **_k: lines.append(text)
    return command.handle(older_than=24, force=True, store=store), lines


def test_a_clean_sweep_exits_zero() -> None:
    store = _Store()

    code, _lines = _purge(store)

    assert code == 0
    assert store.deleted == [1, 2]


def test_a_sweep_that_left_an_undeletable_tracker_row_exits_one() -> None:
    store = _Store(undeletable={2})

    code, _lines = _purge(store)

    assert code == 1
    assert store.deleted == [1]  # the sweep still went on past the failure
