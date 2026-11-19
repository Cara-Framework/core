"""A job's idempotency key includes the tenancy it runs under.

The key used to be the class plus the job's parameters only. A job whose
parameters are not global ids (a period key, a source, or none at all) then
shared ONE result cache and ONE lease across every workspace: workspace B's
run waited on, or was answered by, workspace A's. Jobs worked around it by
carrying a second ``tenant_id`` in their payload, which could disagree with
the tenant the signed envelope armed (B3 isolation sweep, 2026-11-19).
"""

from __future__ import annotations

from cara.context import Tenancy
from cara.queues.idempotency import MakesIdempotentBase


class _MonthlyJob(MakesIdempotentBase):
    idempotency_params = ("period_key",)

    def __init__(self, period_key: str) -> None:
        super().__init__()
        self.period_key = period_key


def _key(job: MakesIdempotentBase, scope) -> str:
    with scope:
        return job.generate_idempotency_key()


def test_two_workspaces_get_two_keys() -> None:
    job = _MonthlyJob("2026-10")

    assert _key(job, Tenancy.as_tenant(1)) != _key(job, Tenancy.as_tenant(2))


def test_central_and_tenant_runs_are_distinct() -> None:
    job = _MonthlyJob("2026-10")

    assert _key(job, Tenancy.central()) != _key(job, Tenancy.as_tenant(1))


def test_the_same_workspace_keeps_one_key() -> None:
    first = _MonthlyJob("2026-10")
    again = _MonthlyJob("2026-10")

    assert _key(first, Tenancy.as_tenant(7)) == _key(again, Tenancy.as_tenant(7))
