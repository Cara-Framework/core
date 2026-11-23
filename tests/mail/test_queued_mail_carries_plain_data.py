"""A queued mail crosses the broker as plain data and is rebuilt on the worker.

Live defect: ``SendMailableJob`` carried the live ``Mailable`` (view data,
often models, included). The signed JSON envelope refuses anything but
JSON primitives by design, so NO queued mail was ever dispatched, and
``MailMessage.queue()`` hid that behind ``except Exception: return False``.
Now ``SendMailableJob.for_mailable`` builds and renders the mailable at
dispatch and carries only its class path plus the rendered message; the
worker rebuilds a ``Mailable`` from that payload. ``MailMessage.queue()``
raises on failure and answers True only once the job is queued; the
mail queue (``default_queue = "emails"`` or ``on_queue()``) reaches
``job.queue``, the attribute the driver reads.
"""

from __future__ import annotations

import importlib
import json

import pytest

from cara.configuration import Configuration
from cara.exceptions import InvalidArgumentException
from cara.mail import Mailable, MailMessage
from cara.mail.jobs import SendMailableJob
from cara.queues import instantiate_job
from cara.queues.serializers import SignedJsonJobSerializer

mailable_module = importlib.import_module("cara.mail.Mailable")
mail_job_module = importlib.import_module("cara.mail.jobs.SendMailableJob")
message_module = importlib.import_module("cara.mail.MailMessage")

_KEYS = {"current": "queued-mail-signing-key-" * 3}


class _User:
    """A live object (a model, typically) that must never be queued."""

    def __init__(self, name: str) -> None:
        self.name = name


class WelcomeMail(Mailable):
    def __init__(self, user: _User) -> None:
        super().__init__()
        self.user = user

    def build(self):
        return (
            self.to("ada@example.com")
            .from_("hello@app.example")
            .subject("Welcome")
            .view("mail.welcome", {"user": self.user})
        )


class _BrokenMail(Mailable):
    def build(self):
        return self.to("ada@example.com").html("<p>x</p>").text(_User("not plain"))


@pytest.fixture(autouse=True)
def product_configuration(monkeypatch):
    """A product trusts its own package for queued classes; here, this one."""
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty().set("queue.job_allowed_prefixes", ["app.jobs", "tests.mail"])
    monkeypatch.setattr(
        mailable_module,
        "render_mail_view",
        lambda _app, view, data: f"<p>{view}: hello {data['user'].name}</p>",
    )


def test_for_mailable_renders_at_dispatch_and_carries_only_plain_data() -> None:
    job = SendMailableJob.for_mailable(WelcomeMail(_User("Ada")))

    assert job.mailable_class == f"{__name__}.WelcomeMail"
    assert job.mailable_data["html"] == "<p>mail.welcome: hello Ada</p>"
    assert job.mailable_data["to"] == ["ada@example.com"]
    assert "view_data" not in job.mailable_data and "view" not in job.mailable_data
    json.dumps(job.mailable_data)  # plain JSON, nothing live inside


def test_the_mail_queue_reaches_the_attribute_the_driver_reads() -> None:
    assert SendMailableJob.for_mailable(WelcomeMail(_User("Ada"))).queue == "emails"

    mailable = WelcomeMail(_User("Ada")).on_queue("priority-mail")
    assert SendMailableJob.for_mailable(mailable).queue == "priority-mail"


def test_non_plain_message_data_is_refused_at_dispatch_naming_the_field() -> None:
    with pytest.raises(InvalidArgumentException, match=r"mailable_data\.text"):
        SendMailableJob.for_mailable(_BrokenMail())


def _round_trip(job: SendMailableJob) -> SendMailableJob:
    body = SignedJsonJobSerializer.serialize(
        {
            "obj": job,
            "args": (),
            "callback": "handle",
            "created": "2026-11-23T00:00:00Z",
            "job_id": "11111111-1111-4111-8111-111111111111",
            "db_job_id": 1,
            "timeout_seconds": 300,
            "_tenant": 7,
            "_tenant_mode": "tenant",
            "queue": job.queue,
            "priority": "default",
            "dispatched_at": "2026-11-23T00:00:00Z",
        },
        signing_key_id="current",
        signing_keys=_KEYS,
        issued_at=1_763_856_000,
    )
    decoded = SignedJsonJobSerializer.deserialize(
        body, signing_keys=_KEYS, now=1_763_856_001
    )
    return instantiate_job(object(), decoded["obj"], (), decoded["init_kwargs"])


@pytest.mark.asyncio
async def test_the_job_survives_the_signed_envelope_and_the_worker_sends_it(
    monkeypatch,
) -> None:
    sent: list[tuple[Mailable, str | None]] = []

    class _Mail:
        @staticmethod
        def _send_now(mailable, driver_name):
            sent.append((mailable, driver_name))
            return True

    monkeypatch.setattr(mail_job_module, "Mail", _Mail)

    rebuilt = _round_trip(SendMailableJob.for_mailable(WelcomeMail(_User("Ada")), "smtp"))
    await rebuilt.handle()

    ((mailable, driver_name),) = sent
    assert type(mailable) is Mailable
    assert driver_name == "smtp"
    data = mailable.to_dict()
    assert data["to"] == ["ada@example.com"]
    assert data["subject"] == "Welcome"
    assert data["html"] == "<p>mail.welcome: hello Ada</p>"


def test_mail_message_queue_returns_true_once_queued(monkeypatch) -> None:
    dispatched: list[SendMailableJob] = []
    monkeypatch.setattr(
        message_module,
        "Queue",
        type("Q", (), {"dispatch": staticmethod(dispatched.append)}),
    )
    manager = type("M", (), {"application": None})()

    message = MailMessage(manager).to("ada@example.com").subject("Hi").html("<p>hi</p>")

    assert message.queue() is True
    (job,) = dispatched
    assert job.mailable_class == "cara.mail.Mailable.Mailable"
    assert job.mailable_data["subject"] == "Hi"


def test_mail_message_queue_raises_when_the_dispatch_fails(monkeypatch) -> None:
    def refuse(_job):
        raise RuntimeError("broker unavailable")

    monkeypatch.setattr(
        message_module, "Queue", type("Q", (), {"dispatch": staticmethod(refuse)})
    )
    message = MailMessage(type("M", (), {"application": None})()).to("a@example.com")

    with pytest.raises(RuntimeError, match="broker unavailable"):
        message.html("<p>hi</p>").queue()
