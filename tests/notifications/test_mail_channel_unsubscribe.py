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
        "preferences_url": "https://app.example/account#notifications",
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
    assert manager.message.view_data["unsubscribe_url"] == (
        "https://app.example/account#notifications"
    )
    assert "List-Unsubscribe" not in manager.message.headers_value
