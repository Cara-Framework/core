"""A hook child that dies before it claims its row still spends an attempt.

``queue:hooks`` no longer claims a row before it spawns the child: the child
(``queue:hook --job-id``) claims it itself, so a row that needs a child keeps
its lease free for that child. A child that cannot even boot — an import that
fails, an interpreter killed on start — exits nonzero having claimed nothing,
and the parent's ``defer_terminal_hook_process_failure`` then met a row with
no lease token and raised ``DeliveryLeaseLost``. That aborted the whole hooks
iteration ("queue-hooks iteration failed; durable rows remain pending"), spent
no attempt and set no backoff, so the next iteration ~2s later booted the same
child for the same row first: live 2026-11-12 08:18-08:21, delivery 5bbe806f
(a dead-lettered ``PullChannelListingsJob``), 88 failed iterations in three
minutes while a mid-edit tree could not import, every other due row in the
batch starved behind it.

The parent now records the failure itself — one attempt, the backoff ladder,
quarantine at the bound — under a lease of its own, so the row waits out its
backoff like any other deferred hook. A row whose previous deferral has
expired is the same case: the child never claimed it this time.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pendulum

from cara.queues.delivery import QueueJobDeliveryStore


class _HookRowDB:
    """One outbox row behind ``select_one``/``statement``, applying the
    deferral UPDATE the way PostgreSQL would: only when its WHERE matches."""

    def __init__(self, **row):
        self.row = {
            "post_hooks_completed_at": None,
            "post_hooks_lease_token": None,
            "post_hooks_lease_expires_at": None,
            "post_hooks_attempts": 0,
            "post_hooks_quarantined_at": None,
            "post_hooks_last_error": None,
            **row,
        }
        self.statements: list[tuple[str, list]] = []

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, _sql, _params=None):
        return dict(self.row)

    def statement(self, sql, params):
        self.statements.append((sql, list(params)))
        if sql.rstrip().endswith("post_hooks_lease_token IS NULL"):
            matched = self.row["post_hooks_lease_token"] is None
        else:
            matched = self.row["post_hooks_lease_token"] == params[-1]
        if not matched or self.row["post_hooks_completed_at"] is not None:
            return 0
        if "post_hooks_quarantined_at = %s" in sql:
            attempts, quarantined_at, error = params[:3]
            self.row.update(
                post_hooks_attempts=attempts,
                post_hooks_quarantined_at=quarantined_at,
                post_hooks_lease_token=None,
                post_hooks_lease_expires_at=None,
                post_hooks_last_error=error,
            )
            return 1
        attempts, expires_at, error = params[:3]
        self.row.update(
            post_hooks_attempts=attempts,
            post_hooks_lease_expires_at=expires_at,
            post_hooks_last_error=error,
        )
        if "post_hooks_lease_token = %s" in sql.split(" WHERE ", 1)[0]:
            self.row["post_hooks_lease_token"] = params[4]
        return 1


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _store(db, **options):
    return QueueJobDeliveryStore(
        _Application(db),
        SimpleNamespace(),
        {"canonical_queues": ("sync", "connector"), **options},
    )


def test_a_child_that_never_claimed_is_deferred_not_lost():
    """Pinned wrong behaviour: this raised ``DeliveryLeaseLost``."""
    db = _HookRowDB()
    before = pendulum.now("UTC")

    outcome = _store(db).defer_terminal_hook_process_failure(
        "hook-job-id",
        error="isolated terminal-hook process exited with status 1",
    )

    assert outcome == "deferred"
    assert db.row["post_hooks_attempts"] == 1
    # Backed off under a lease of the parent's own: not due again at once.
    assert db.row["post_hooks_lease_token"]
    assert db.row["post_hooks_lease_expires_at"] >= before.add(seconds=59)
    assert "status 1" in db.row["post_hooks_last_error"]


def test_a_stale_deferral_counts_the_next_boot_failure():
    """The row's previous deferral has run out and the child died before it
    claimed again: the old ``post_hooks_last_error`` is not this child's
    self-deferral, so the failure counts (it used to read as
    ``already_deferred`` and leave the row due, with no new backoff)."""
    expired = pendulum.now("UTC").subtract(seconds=5)
    db = _HookRowDB(
        post_hooks_lease_token="parent-lease",
        post_hooks_lease_expires_at=expired,
        post_hooks_attempts=1,
        post_hooks_last_error="isolated terminal-hook process exited with status 1",
    )

    outcome = _store(db).defer_terminal_hook_process_failure(
        "hook-job-id",
        error="isolated terminal-hook process exited with status 1",
    )

    assert outcome == "deferred"
    assert db.row["post_hooks_attempts"] == 2
    assert db.row["post_hooks_lease_expires_at"] > pendulum.now("UTC")


def test_a_child_that_never_boots_is_quarantined_at_the_bound():
    db = _HookRowDB(post_hooks_attempts=1)

    store = _store(db, delivery_hook_max_attempts=2)

    outcome = store.defer_terminal_hook_process_failure(
        "hook-job-id",
        error="isolated terminal-hook process exited with status 1",
    )

    assert outcome == "quarantined"
    assert db.row["post_hooks_attempts"] == 2
    assert isinstance(db.row["post_hooks_quarantined_at"], pendulum.DateTime)
    assert db.row["post_hooks_lease_token"] is None


def test_a_live_child_self_deferral_is_still_not_double_counted():
    """Unchanged: the child claimed, deferred itself (a future expiry and its
    own error), then exited nonzero — the parent adds nothing."""
    db = _HookRowDB(
        post_hooks_lease_token="child-lease",
        post_hooks_lease_expires_at=pendulum.now("UTC").add(seconds=60),
        post_hooks_attempts=2,
        post_hooks_last_error="child already deferred",
    )

    outcome = _store(db).defer_terminal_hook_process_failure(
        "hook-job-id",
        error="child exited nonzero",
    )

    assert outcome == "already_deferred"
    assert db.statements == []
