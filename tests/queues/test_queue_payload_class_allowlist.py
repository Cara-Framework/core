"""A queued mail/notification class path is bounded BEFORE anything is imported.

``QueuePayload.resolve_class`` imported every module prefix the path named
and only then checked the class, and ``SendNotificationJob`` rebuilt with
``base=object`` — any importable class with a callable
``from_queue_payload`` was accepted. The envelope HMAC was the only thing
between a forged payload and an arbitrary import. Now the path must lie
under the queued-job allowlist (plus ``cara.mail`` / ``cara.notifications``)
before an import runs, and the class must implement the real contract.
"""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import InvalidArgumentException, QueueException
from cara.mail.Mailable import Mailable
from cara.notifications import BaseNotification, Notifiable
from cara.notifications.jobs import SendNotificationJob
from cara.queues import QueuePayload

payload_module = importlib.import_module("cara.queues.QueuePayload")
resolver_module = importlib.import_module("cara.queues.JobClassResolver")

_HERE = __name__


class _Recipient(Notifiable):
    id = 7

    @classmethod
    def find(cls, key):
        recipient = cls()
        recipient.id = key
        return recipient


class _Ready(BaseNotification):
    def via(self, notifiable) -> list[str]:
        return ["mail"]


class _Impostor:
    """Has the rebuild hook but implements no contract."""

    @classmethod
    def from_queue_payload(cls, data):
        return cls()

    def to_queue_payload(self):
        return {}


@pytest.fixture(autouse=True)
def allowlist(monkeypatch):
    """The product allowlist: this test module's package is a trusted root."""
    root = _HERE.rsplit(".", 1)[0]
    monkeypatch.setattr(
        resolver_module,
        "config",
        lambda key, default=None: [root] if key == "queue.job_allowed_prefixes" else None,
    )
    return root


@pytest.fixture
def imports(monkeypatch):
    seen: list[str] = []
    real = importlib.import_module

    def _recording(name, *args, **kwargs):
        seen.append(name)
        return real(name, *args, **kwargs)

    monkeypatch.setattr(payload_module.importlib, "import_module", _recording)
    return seen


def test_a_path_outside_the_allowlist_is_refused_before_any_import(imports):
    with pytest.raises(QueueException, match="allowlist"):
        QueuePayload.resolve_class("os.path.Popen", Mailable)
    with pytest.raises(QueueException, match="allowlist"):
        QueuePayload.resolve_class("subprocess.Popen", BaseNotification)
    assert imports == []


def test_object_is_never_an_accepted_contract(imports):
    with pytest.raises(QueueException, match="contract"):
        QueuePayload.resolve_class(f"{_HERE}._Impostor", object)
    assert imports == []


def test_the_framework_contract_packages_are_trusted():
    assert QueuePayload.resolve_class("cara.mail.Mailable.Mailable", Mailable) is Mailable


def test_a_trusted_class_resolves_against_its_contract():
    assert QueuePayload.resolve_class(f"{_HERE}._Ready", BaseNotification) is _Ready


def test_the_notification_job_requires_the_notification_contract():
    with pytest.raises(QueueException, match="subclass of BaseNotification"):
        SendNotificationJob._rebuild(f"{_HERE}._Impostor", {}, "notification")


def test_the_notification_job_requires_the_notifiable_contract():
    with pytest.raises(QueueException, match="subclass of NotifiableContract"):
        SendNotificationJob._rebuild(f"{_HERE}._Impostor", {}, "notifiable")
    rebuilt = SendNotificationJob._rebuild(
        f"{_HERE}._Recipient", {"key": 9}, "notifiable"
    )
    assert isinstance(rebuilt, _Recipient) and rebuilt.id == 9


def test_dispatch_refuses_an_object_that_is_not_the_contract():
    with pytest.raises(InvalidArgumentException, match="NotifiableContract"):
        SendNotificationJob.payload_of(_Impostor(), "notifiable")


def test_dispatch_refuses_a_class_the_worker_would_not_import(monkeypatch):
    monkeypatch.setattr(
        resolver_module,
        "config",
        lambda key, default=None: (
            ["app.jobs"] if key == "queue.job_allowed_prefixes" else None
        ),
    )
    with pytest.raises(QueueException, match="allowlist"):
        SendNotificationJob.payload_of(_Recipient(), "notifiable")
