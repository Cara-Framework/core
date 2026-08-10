"""``schema:plan --rehearse``: does the derived SQL actually run?

Classification says what an operation costs; preflight says whether the rows
allow it. Neither executes anything, and a plan can pass both and still be
rejected by the server — a non-IMMUTABLE index expression is the canonical
case, and it reads as perfectly ordinary in a review.

These tests hold the rehearsal to three promises: it runs the REAL apply
rather than a rehearsal-flavoured copy, it never leaves a scratch database
behind, and a failing rehearsal fails the command.
"""

from __future__ import annotations

import pytest

from cara.exceptions import ScratchDatabaseException
from cara.schema import ADDITIVE, Operation, Scratch


def _operation(key="users.phone"):
    return Operation(
        kind="add_column",
        table="users",
        key=key,
        forward_sql='ALTER TABLE "users" ADD COLUMN "phone" VARCHAR(255)',
        reverse_sql='ALTER TABLE "users" DROP COLUMN "phone"',
        safety=ADDITIVE,
        reason="nullable column declared by the model",
    )


class _Plan:
    """The command with only its outside world replaced."""

    def __init__(self, apply_exit=0, prepare_error=None, allow_destructive=False):
        from cara.commands.core.SchemaPlanCommand import SchemaPlanCommand

        self.command = SchemaPlanCommand.__new__(SchemaPlanCommand)
        self.command.option = lambda name: (
            allow_destructive if name == "allow_destructive" else None
        )
        self.messages: list[str] = []
        for level in ("info", "warning", "error", "success"):
            setattr(self.command, level, self.messages.append)

        self.apply_exit = apply_exit
        self.prepare_error = prepare_error
        self.created: list[str] = []
        self.dropped: list[str] = []
        self.cloned: list[tuple] = []
        self.craft: list[list[str]] = []

    def install(self, monkeypatch):
        import cara.commands.core.SchemaPlanCommand as module

        monkeypatch.setattr(
            module.Scratch,
            "connection_params",
            lambda config: {"database": "synkronus", "host": "h", "user": "u"},
        )
        monkeypatch.setattr(
            module.Scratch, "recreate", lambda p, n: self.created.append(n)
        )
        monkeypatch.setattr(module.Scratch, "drop", lambda p, n: self.dropped.append(n))

        def _clone(params, source, name):
            if self.prepare_error:
                raise ScratchDatabaseException(self.prepare_error)
            self.cloned.append((source, name))

        monkeypatch.setattr(module.Scratch, "clone_structure", _clone)

        def _craft(arguments, name, cwd):
            self.craft.append(arguments)
            return self.apply_exit

        monkeypatch.setattr(module.Scratch, "run_craft", _craft)
        return self

    def rehearse(self, operations=None):
        return self.command._rehearse(
            "abc123", operations if operations is not None else [_operation()], []
        )

    @property
    def text(self):
        return "\n".join(self.messages)


def test_the_rehearsal_runs_the_real_apply_not_a_copy_of_it(monkeypatch):
    """A rehearsal with its own executor would prove that executor works,
    which is not the question. It spawns ``schema:apply`` itself, through the
    plan artifact, so apply's own staleness gate also confirms the clone
    derives the same plan."""
    plan = _Plan().install(monkeypatch)

    assert plan.rehearse() == 0
    assert len(plan.craft) == 1
    assert plan.craft[0][0] == "schema:apply"
    assert "--plan" in plan.craft[0]


def test_the_scratch_is_created_from_the_deployed_shape_and_then_dropped(monkeypatch):
    plan = _Plan().install(monkeypatch)

    plan.rehearse()

    assert plan.created == ["synkronus_rehearsal"]
    assert plan.cloned == [("synkronus", "synkronus_rehearsal")]
    assert plan.dropped == ["synkronus_rehearsal"]


def test_a_failed_rehearsal_still_drops_the_scratch(monkeypatch):
    """The failure path is the one that leaves debris. A scratch surviving a
    failed rehearsal is a database named after production, on the production
    server, that nobody is watching."""
    plan = _Plan(apply_exit=1).install(monkeypatch)

    assert plan.rehearse() == 1
    assert plan.dropped == ["synkronus_rehearsal"]
    assert "REHEARSAL FAILED" in plan.text


def test_a_scratch_that_cannot_be_prepared_is_reported_not_rehearsed(monkeypatch):
    """No clone means no rehearsal — and saying so beats running the plan
    against whatever the scratch happens to contain."""
    plan = _Plan(prepare_error="pg_dump is not on PATH").install(monkeypatch)

    assert plan.rehearse() == 2
    assert plan.craft == []
    assert "pg_dump is not on PATH" in plan.text
    assert plan.dropped == ["synkronus_rehearsal"]


def test_destructive_permission_carries_into_the_rehearsal(monkeypatch):
    """By the time a rehearsal runs, plan has already exited non-zero unless
    the operator named the intent — so the child must be allowed to run the
    same plan the operator accepted, or the rehearsal tests a shorter one."""
    plan = _Plan(allow_destructive=True).install(monkeypatch)

    plan.rehearse()

    assert "--allow_destructive" in plan.craft[0]


def test_the_rehearsal_never_targets_the_configured_database():
    """Everything in this path ends in DROP DATABASE."""
    with pytest.raises(ScratchDatabaseException, match="refusing to drop it"):
        Scratch.validate_name("synkronus", "synkronus")


def test_an_exotic_configured_name_still_yields_a_boring_scratch():
    """``synkronus.io`` is a real configured database name; the scratch is
    interpolated into DDL as an identifier and must stay plain."""
    name = Scratch.derive_name("synkronus.io", "rehearsal")

    assert name == "synkronus_io_rehearsal"
    Scratch.validate_name(name, "synkronus.io")


def test_an_unsafe_explicit_name_is_refused_on_its_own_merits():
    with pytest.raises(ScratchDatabaseException, match="plain lowercase identifier"):
        Scratch.validate_name('evil"; DROP DATABASE x --', "synkronus")
