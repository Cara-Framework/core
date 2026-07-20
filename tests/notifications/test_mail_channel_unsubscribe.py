from __future__ import annotations

import hashlib
import hmac
import importlib
from types import SimpleNamespace
from urllib.parse import urlencode

from cara.notifications.channels.MailChannel import MailChannel

USER_PUBLIC_ID = "USR01ARZ3NDEKTSV4RRFFQ69G5FAV"
SECRET = "unsubscribe-secret-at-least-32-bytes"  # gitleaks:allow


class _Message:
    def __init__(self) -> None:
        self.headers_value: dict[str, str] = {}
        self.view_data: dict = {}

    def subject(self, value):
        return self

    def text(self, value):
        return self

    def html(self, value):
        return self

    def view(self, template, data):
        self.view_data = data
        return self

    def headers(self, values):
        self.headers_value.update(values)
        return self

    def from_(self, address, name=None):
        return self

    def reply_to(self, address):
        return self

    def attach(self, name, path):
        return self

    def send(self):
        return True


class _MailManager:
    def __init__(self) -> None:
        self.message = _Message()

    def to(self, recipient):
        return self.message


class _Notification:
    def to_mail(self, notifiable):
        return {
            "subject": "Inventory alert",
            "view": "mail.notifications.inventory",
            "data": {},
            "headers": {"X-Message-Class": "inventory"},
        }


def test_notification_mail_adds_confirmation_link_and_rfc8058_headers(
    monkeypatch,
) -> None:
    configuration = importlib.import_module("cara.configuration")
    values = {
        "app.frontend_url": "https://app.example",
        "app.preferences_url": "https://app.example/notifications/preferences",
        "app.unsubscribe_confirm_url": "https://app.example/unsubscribe",
        "app.unsubscribe_url": "https://app.example/api/unsubscribe",
        "app.unsubscribe_secret": SECRET,
    }
    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: values.get(key, default),
    )
    manager = _MailManager()
    notifiable = SimpleNamespace(
        id=41,
        public_id=USER_PUBLIC_ID,
        email="user@example.com",
    )

    assert MailChannel(manager).send(notifiable, _Notification()) is True

    token = hmac.new(
        SECRET.encode(),
        f"{USER_PUBLIC_ID}:user@example.com".encode(),
        hashlib.sha256,
    ).hexdigest()
    query = urlencode({"user": USER_PUBLIC_ID, "token": token})
    assert manager.message.view_data == {
        "frontend_url": "https://app.example",
        "preferences_url": "https://app.example/notifications/preferences",
        "unsubscribe_url": f"https://app.example/unsubscribe?{query}",
        "unsubscribe_one_click_url": (
            f"https://app.example/api/unsubscribe?{query}"
        ),
    }
    assert manager.message.headers_value == {
        "X-Message-Class": "inventory",
        "List-Unsubscribe": (
            f"<https://app.example/api/unsubscribe?{query}>"
        ),
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def test_notification_mail_never_falls_back_to_internal_numeric_user_id(
    monkeypatch,
) -> None:
    configuration = importlib.import_module("cara.configuration")
    values = {
        "app.frontend_url": "https://app.example",
        "app.preferences_url": "https://app.example/notifications/preferences",
        "app.unsubscribe_confirm_url": "https://app.example/unsubscribe",
        "app.unsubscribe_url": "https://app.example/api/unsubscribe",
        "app.unsubscribe_secret": SECRET,
    }
    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: values.get(key, default),
    )
    manager = _MailManager()
    notifiable = SimpleNamespace(id=41, email="user@example.com")

    assert MailChannel(manager).send(notifiable, _Notification()) is True
    # No SIGNED link is minted for an unsignable recipient, and the internal
    # row id never stands in for the opaque public_id — signing it would leak
    # the id space into mailboxes and mail logs, and the verifier resolves
    # users by public_id anyway.
    view_data = manager.message.view_data
    assert "41" not in view_data["unsubscribe_url"]
    assert "token=" not in view_data["unsubscribe_url"]
    # The reader still gets a real opt-out: the preferences page carries the
    # controls and needs no signature. Dropping the link entirely would render
    # the template's ``default('#')`` — a dead footer link, and a CAN-SPAM
    # failure that looks fine in review.
    assert view_data["unsubscribe_url"] == values["app.preferences_url"]
    # RFC 8058 promises one POST opts the reader out; the preferences page
    # cannot deliver that, so the header must not claim it.
    assert "unsubscribe_one_click_url" not in view_data
    assert "List-Unsubscribe" not in manager.message.headers_value


def test_visible_unsubscribe_falls_back_to_the_processor_without_a_confirm_page(
    monkeypatch,
) -> None:
    """A product may have no confirmation PAGE — only the processor.

    A UI-less processor that answers GET is a legitimate human destination, so
    it becomes the visible link rather than leaving the reader with no link at
    all. Pinned because the honest-null reading of an unset
    ``app.unsubscribe_confirm_url`` silently shipped mail whose only opt-out
    was an RFC 8058 header no human can click.
    """
    configuration = importlib.import_module("cara.configuration")
    values = {
        "app.frontend_url": "https://app.example",
        "app.unsubscribe_url": "https://app.example/api/unsubscribe",
        "app.unsubscribe_secret": SECRET,
    }
    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: values.get(key, default),
    )
    manager = _MailManager()
    notifiable = SimpleNamespace(
        id=41,
        public_id=USER_PUBLIC_ID,
        email="user@example.com",
    )

    assert MailChannel(manager).send(notifiable, _Notification()) is True

    token = hmac.new(
        SECRET.encode(),
        f"{USER_PUBLIC_ID}:user@example.com".encode(),
        hashlib.sha256,
    ).hexdigest()
    query = urlencode({"user": USER_PUBLIC_ID, "token": token})
    expected = f"https://app.example/api/unsubscribe?{query}"
    view_data = manager.message.view_data
    assert view_data["unsubscribe_url"] == expected
    # One endpoint answers both the human GET and the one-click POST, so a
    # click and an auto-POST must do the same thing.
    assert view_data["unsubscribe_one_click_url"] == expected
    assert manager.message.headers_value["List-Unsubscribe"] == f"<{expected}>"


def test_one_click_is_dropped_when_the_notification_supplies_its_own_link(
    monkeypatch,
) -> None:
    """``List-Unsubscribe`` must never point somewhere the reader cannot see.

    If a notification overrides the visible link, advertising the framework's
    processor in the header would let a mail client opt the reader out via a
    URL that never appeared in the message.
    """
    configuration = importlib.import_module("cara.configuration")
    values = {
        "app.unsubscribe_url": "https://app.example/api/unsubscribe",
        "app.unsubscribe_secret": SECRET,
    }
    monkeypatch.setattr(
        configuration,
        "config",
        lambda key, default=None: values.get(key, default),
    )

    class _OverridingNotification:
        def to_mail(self, notifiable):
            return {
                "view": "mail.notifications.inventory",
                "data": {"unsubscribe_url": "https://app.example/custom?u=1"},
            }

    manager = _MailManager()
    notifiable = SimpleNamespace(
        id=41,
        public_id=USER_PUBLIC_ID,
        email="user@example.com",
    )

    assert MailChannel(manager).send(notifiable, _OverridingNotification()) is True
    view_data = manager.message.view_data
    assert view_data["unsubscribe_url"] == "https://app.example/custom?u=1"
    assert "unsubscribe_one_click_url" not in view_data
    assert "List-Unsubscribe" not in manager.message.headers_value
