"""A retry is the signed work, plus only the progress a job declares.

``JobProcessor._requeue_with_delay`` re-serialised the instance that had just
failed, so every constructor attribute the failed run changed (a page
cursor it advanced, an id it adopted, a list it trimmed) shipped on the
retry: the retry could be different work from the dispatch that was signed
(a Cheapa job that rewrote a constructor field changed its retry's
idempotency key that way). Now every retry is rebuilt from the signed
``init_kwargs``; carrying progress is an explicit
``ShouldQueue.retry_carries`` declaration, and the default ``()`` carries
nothing.
"""

from __future__ import annotations

import builtins
from types import SimpleNamespace

import pytest

from cara.commands.core.JobProcessor import JobProcessor
from cara.queues import Bus
from cara.queues.contracts import ShouldQueue


class _Pull(ShouldQueue):
    def __init__(self, channel_id: int, cursor: str | None = None, run_id=None):
        self.channel_id = channel_id
        self.cursor = cursor
        self.run_id = run_id

    async def handle(self) -> None:
        return None


class _ResumingPull(_Pull):
    retry_carries = ("run_id",)


class _PlainPull(_Pull):
    retry_carries = ()


class _Bagged(ShouldQueue):
    retry_carries = ("marketplace",)

    def __init__(self, url: str, **kwargs):
        self.url = url
        self.kwargs = kwargs

    async def handle(self) -> None:
        return None


class _App:
    def __init__(self, queue_service):
        self.queue_service = queue_service
        self.logger = SimpleNamespace(
            debug=lambda *a, **k: None,
            error=lambda *a, **k: None,
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
        )

    def make(self, key):
        return {"queue": self.queue_service, "logger": self.logger}[key]


def _retried_params(monkeypatch, instance, signed: dict) -> dict:
    """Run the retry router and return what the retry would serialise."""
    shipped: list = []
    queue_service = SimpleNamespace(
        driver=lambda: SimpleNamespace(),
        later=lambda delay, job, **options: shipped.append(job),
    )
    monkeypatch.setattr(builtins, "app", lambda: _App(queue_service), raising=False)
    JobProcessor._requeue_with_delay(
        SimpleNamespace(basic_ack=lambda *, delivery_tag: None),
        SimpleNamespace(delivery_tag=1),
        {
            "attempts": 0,
            "queue": "q",
            "job_id": "j",
            "db_job_id": 1,
            "_tenant": 5,
            "_tenant_mode": "tenant",
            "_otel": {},
            "init_kwargs": signed,
        },
        instance,
        RuntimeError("boom"),
        "q",
        "lease",
        SimpleNamespace(ensure_retry_progress_strict=lambda job_id: None),
        1,
    )
    (job,) = shipped
    return Bus.get_dispatch_params(job)


def _failed_run(job_class):
    job = job_class(7, cursor="page-1")
    signed = Bus.get_dispatch_params(job)
    job.cursor = "page-9"
    job.run_id = 42
    return job, signed


def test_a_job_that_carries_nothing_retries_the_signed_arguments(monkeypatch):
    job, signed = _failed_run(_PlainPull)

    assert _retried_params(monkeypatch, job, signed) == signed


def test_only_the_declared_attribute_keeps_the_failed_runs_value(monkeypatch):
    job, signed = _failed_run(_ResumingPull)

    assert _retried_params(monkeypatch, job, signed) == {
        "channel_id": 7,
        "cursor": "page-1",
        "run_id": 42,
    }


def test_an_undeclared_job_retries_the_signed_arguments(monkeypatch):
    job, signed = _failed_run(_Pull)

    assert _retried_params(monkeypatch, job, signed) == signed


def test_the_live_instance_is_not_touched_by_the_rebuild(monkeypatch):
    job, signed = _failed_run(_PlainPull)

    _retried_params(monkeypatch, job, signed)

    assert (job.cursor, job.run_id) == ("page-9", 42)


def test_a_stored_kwargs_bag_is_rebuilt_without_sharing_the_live_dict(monkeypatch):
    shared = {"marketplace": None, "depth": 1}
    job = _Bagged("https://x", **shared)
    job.kwargs = shared
    signed = Bus.get_dispatch_params(job)
    shared["marketplace"] = "amazon-de"
    shared["depth"] = 5

    params = _retried_params(monkeypatch, job, signed)

    assert params == {"url": "https://x", "marketplace": "amazon-de", "depth": 1}
    assert shared == {"marketplace": "amazon-de", "depth": 5}


def test_a_parameter_the_run_created_does_not_appear_on_the_retry():
    job = _PlainPull(7)
    del job.run_id
    signed = Bus.get_dispatch_params(job)
    job.run_id = 42

    assert "run_id" not in Bus.get_dispatch_params(Bus.with_dispatch_params(job, signed))


def test_a_single_name_declared_as_a_string_carries_that_name(monkeypatch):
    class _OneName(_Pull):
        retry_carries = "run_id"

    job, signed = _failed_run(_OneName)

    assert _OneName.retry_carries == ("run_id",)
    assert _retried_params(monkeypatch, job, signed)["run_id"] == 42


@pytest.mark.parametrize("declared", [None, 3, ("run_id", None), ("",)])
def test_a_malformed_declaration_is_refused_when_the_class_is_defined(declared):
    with pytest.raises(TypeError, match="retry_carries"):
        type("_Malformed", (_Pull,), {"retry_carries": declared})
