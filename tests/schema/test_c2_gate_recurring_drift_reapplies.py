"""The same drift arising twice is applied twice, never skipped as "done".

A plan id is a hash of its operations. When a guard trigger is disabled again,
or the same index is dropped again, the derived plan carries the id of a plan
that already finished. The ledger rows of that finished plan are history, not
a resume: reading them as "already applied" skipped every operation the
database still needed, printed "Applied 0" and left the guard off (seen on a
scratch cluster: audit_log_immutable stayed DISABLED after the second apply).
"""

from __future__ import annotations

from cara.schema import plan_id
from tests.schema.test_schema_evolve_resume_and_fill import (
    _COLUMN,
    _OTHER,
    _apply,
    _Ledger,
)


def _ran(ledger: _Ledger) -> list[str]:
    return [sql for sql, _ in ledger.statements if sql.startswith("ALTER TABLE")]


def test_a_recurring_drift_is_applied_again_from_its_artifact(monkeypatch, tmp_path):
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key, _OTHER.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, reviewed, ledger)

    assert code == 0
    assert _ran(ledger) == [_COLUMN.forward_sql, _OTHER.forward_sql]
    assert any("derives it again" in message for message in messages)
    assert any("Applied 2" in message for message in messages)


def test_a_recurring_drift_is_applied_again_without_an_artifact(monkeypatch, tmp_path):
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key, _OTHER.key}})

    code, _messages = _apply(monkeypatch, tmp_path, reviewed, reviewed, ledger, plan=None)

    assert code == 0
    assert _ran(ledger) == [_COLUMN.forward_sql, _OTHER.forward_sql]


def test_a_real_resume_still_skips_what_the_ledger_recorded(monkeypatch, tmp_path):
    """The artifact's other half ran; only the rest derives, and only it runs."""
    reviewed = [_COLUMN, _OTHER]
    ledger = _Ledger(applied={plan_id(reviewed): {_COLUMN.key}})

    code, messages = _apply(monkeypatch, tmp_path, reviewed, [_OTHER], ledger)

    assert code == 0
    assert _ran(ledger) == [_OTHER.forward_sql]
    assert any("Resuming" in message for message in messages)
