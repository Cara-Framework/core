"""
Fake - Enhanced fake services for testing in Cara framework

This file provides fake implementations of services for testing with
integration for current Cara framework features.
"""

from datetime import datetime
from typing import Any, Dict


class FakeService:
    """Base fake service class."""

    def __init__(self):
        """Initialize fake service."""
        self.calls = []
        self.responses = {}

    def record_call(self, method: str, *args, **kwargs):
        """Record a method call."""
        self.calls.append(
            {
                "method": method,
                "args": args,
                "kwargs": kwargs,
                "timestamp": datetime.now(),
            }
        )

    def set_response(self, method: str, response: Any):
        """Set response for a method."""
        self.responses[method] = response

    def get_response(self, method: str, default: Any = None):
        """Get response for a method."""
        return self.responses.get(method, default)

    def assert_called(self, method: str, times: int = None):
        """Assert that method was called."""
        calls = [call for call in self.calls if call["method"] == method]
        if times is not None:
            assert len(calls) == times, (
                f"Expected {times} calls to {method}, got {len(calls)}"
            )
        else:
            assert len(calls) > 0, f"Expected at least one call to {method}, got none"

    def assert_not_called(self, method: str):
        """Assert that method was not called."""
        calls = [call for call in self.calls if call["method"] == method]
        assert len(calls) == 0, f"Expected no calls to {method}, got {len(calls)}"

    def clear_calls(self):
        """Clear recorded calls."""
        self.calls.clear()


class FakeMailer(FakeService):
    """Fake mailer for testing."""

    def __init__(self):
        """Initialize fake mailer."""
        super().__init__()
        self.sent_emails = []

    def send(self, to: str, subject: str, body: str, from_email: str = None, **kwargs):
        """Fake send email."""
        self.record_call("send", to, subject, body, from_email, **kwargs)

        email = {
            "to": to,
            "subject": subject,
            "body": body,
            "from": from_email,
            "timestamp": datetime.now(),
            "kwargs": kwargs,
        }
        self.sent_emails.append(email)
        return True

    def send_template(self, to: str, template: str, data: Dict = None, **kwargs):
        """Fake send template email."""
        self.record_call("send_template", to, template, data, **kwargs)

        email = {
            "to": to,
            "template": template,
            "data": data or {},
            "timestamp": datetime.now(),
            "kwargs": kwargs,
        }
        self.sent_emails.append(email)
        return True

    def assert_sent(self, to: str = None, subject: str = None, count: int = None):
        """Assert that email was sent."""
        matching_emails = self.sent_emails

        if to:
            matching_emails = [
                email for email in matching_emails if email.get("to") == to
            ]

        if subject:
            matching_emails = [
                email for email in matching_emails if email.get("subject") == subject
            ]

        if count is not None:
            assert len(matching_emails) == count, (
                f"Expected {count} emails, got {len(matching_emails)}"
            )
        else:
            assert len(matching_emails) > 0, "Expected at least one email to be sent"

    def assert_not_sent(self, to: str = None, subject: str = None):
        """Assert that email was not sent."""
        matching_emails = self.sent_emails

        if to:
            matching_emails = [
                email for email in matching_emails if email.get("to") == to
            ]

        if subject:
            matching_emails = [
                email for email in matching_emails if email.get("subject") == subject
            ]

        assert len(matching_emails) == 0, (
            f"Expected no emails, got {len(matching_emails)}"
        )

    def clear_sent(self):
        """Clear sent emails."""
        self.sent_emails.clear()
        self.clear_calls()


class FakeQueue(FakeService):
    """Fake queue for testing."""

    def __init__(self):
        """Initialize fake queue."""
        super().__init__()
        self.queued_jobs = []

    def push(self, job: str, data: Dict = None, queue: str = "default", delay: int = 0):
        """Fake push job to queue."""
        self.record_call("push", job, data, queue, delay)

        queued_job = {
            "job": job,
            "data": data or {},
            "queue": queue,
            "delay": delay,
            "timestamp": datetime.now(),
        }
        self.queued_jobs.append(queued_job)
        return True

    def later(self, delay: int, job: str, data: Dict = None, queue: str = "default"):
        """Fake push delayed job to queue."""
        return self.push(job, data, queue, delay)

    def assert_pushed(self, job: str = None, queue: str = None, count: int = None):
        """Assert that job was pushed to queue."""
        matching_jobs = self.queued_jobs

        if job:
            matching_jobs = [j for j in matching_jobs if j.get("job") == job]

        if queue:
            matching_jobs = [j for j in matching_jobs if j.get("queue") == queue]

        if count is not None:
            assert len(matching_jobs) == count, (
                f"Expected {count} jobs, got {len(matching_jobs)}"
            )
        else:
            assert len(matching_jobs) > 0, "Expected at least one job to be queued"

    def assert_not_pushed(self, job: str = None, queue: str = None):
        """Assert that job was not pushed to queue."""
        matching_jobs = self.queued_jobs

        if job:
            matching_jobs = [j for j in matching_jobs if j.get("job") == job]

        if queue:
            matching_jobs = [j for j in matching_jobs if j.get("queue") == queue]

        assert len(matching_jobs) == 0, f"Expected no jobs, got {len(matching_jobs)}"

    def clear_queued(self):
        """Clear queued jobs."""
        self.queued_jobs.clear()
        self.clear_calls()


class FakeNotification(FakeService):
    """Fake notification for testing."""

    def __init__(self):
        """Initialize fake notification."""
        super().__init__()
        self.sent_notifications = []

    def send(self, notifiable, notification):
        """Fake send notification."""
        self.record_call("send", notifiable, notification)

        sent_notification = {
            "notifiable": notifiable,
            "notification": notification,
            "notification_type": notification.__class__.__name__,
            "timestamp": datetime.now(),
        }
        self.sent_notifications.append(sent_notification)
        return True

    def send_now(self, notifiable, notification):
        """Fake send notification immediately."""
        return self.send(notifiable, notification)

    def assert_sent(
        self, notifiable=None, notification_type: str = None, count: int = None
    ):
        """Assert that notification was sent."""
        matching_notifications = self.sent_notifications

        if notifiable:
            matching_notifications = [
                n for n in matching_notifications if n.get("notifiable") == notifiable
            ]

        if notification_type:
            matching_notifications = [
                n
                for n in matching_notifications
                if n.get("notification_type") == notification_type
            ]

        if count is not None:
            assert len(matching_notifications) == count, (
                f"Expected {count} notifications, got {len(matching_notifications)}"
            )
        else:
            assert len(matching_notifications) > 0, (
                "Expected at least one notification to be sent"
            )

    def assert_not_sent(self, notifiable=None, notification_type: str = None):
        """Assert that notification was not sent."""
        matching_notifications = self.sent_notifications

        if notifiable:
            matching_notifications = [
                n for n in matching_notifications if n.get("notifiable") == notifiable
            ]

        if notification_type:
            matching_notifications = [
                n
                for n in matching_notifications
                if n.get("notification_type") == notification_type
            ]

        assert len(matching_notifications) == 0, (
            f"Expected no notifications, got {len(matching_notifications)}"
        )

    def clear_sent(self):
        """Clear sent notifications."""
        self.sent_notifications.clear()
        self.clear_calls()


class FakeEvent(FakeService):
    """Fake event dispatcher for testing."""

    def __init__(self):
        """Initialize fake event dispatcher."""
        super().__init__()
        self.dispatched_events = []

    def dispatch(self, event: str, data: Dict = None):
        """Fake dispatch event."""
        self.record_call("dispatch", event, data)

        dispatched_event = {
            "event": event,
            "data": data or {},
            "timestamp": datetime.now(),
        }
        self.dispatched_events.append(dispatched_event)
        return True

    def fire(self, event: str, data: Dict = None):
        """Fake fire event (alias for dispatch)."""
        return self.dispatch(event, data)

    def assert_dispatched(self, event: str = None, count: int = None):
        """Assert that event was dispatched."""
        matching_events = self.dispatched_events

        if event:
            matching_events = [e for e in matching_events if e.get("event") == event]

        if count is not None:
            assert len(matching_events) == count, (
                f"Expected {count} events, got {len(matching_events)}"
            )
        else:
            assert len(matching_events) > 0, (
                "Expected at least one event to be dispatched"
            )

    def assert_not_dispatched(self, event: str = None):
        """Assert that event was not dispatched."""
        matching_events = self.dispatched_events

        if event:
            matching_events = [e for e in matching_events if e.get("event") == event]

        assert len(matching_events) == 0, (
            f"Expected no events, got {len(matching_events)}"
        )

    def clear_dispatched(self):
        """Clear dispatched events."""
        self.dispatched_events.clear()
        self.clear_calls()


class FakeStorage(FakeService):
    """Fake storage for testing."""

    def __init__(self):
        """Initialize fake storage."""
        super().__init__()
        self.stored_files = {}

    def put(self, path: str, content: str):
        """Fake store file."""
        self.record_call("put", path, content)
        self.stored_files[path] = {"content": content, "timestamp": datetime.now()}
        return True

    def get(self, path: str):
        """Fake get file content."""
        self.record_call("get", path)
        file_info = self.stored_files.get(path)
        return file_info["content"] if file_info else None

    def exists(self, path: str):
        """Fake check if file exists."""
        self.record_call("exists", path)
        return path in self.stored_files

    def delete(self, path: str):
        """Fake delete file."""
        self.record_call("delete", path)
        if path in self.stored_files:
            del self.stored_files[path]
            return True
        return False

    def assert_stored(self, path: str, content: str = None):
        """Assert that file was stored."""
        assert path in self.stored_files, f"File {path} was not stored"

        if content is not None:
            stored_content = self.stored_files[path]["content"]
            assert stored_content == content, (
                f"Expected content '{content}', got '{stored_content}'"
            )

    def assert_not_stored(self, path: str):
        """Assert that file was not stored."""
        assert path not in self.stored_files, f"File {path} was stored"

    def clear_stored(self):
        """Clear stored files."""
        self.stored_files.clear()
        self.clear_calls()


class FakeCache(FakeService):
    """Fake cache for testing."""

    def __init__(self):
        """Initialize fake cache."""
        super().__init__()
        self.cached_items = {}

    def put(self, key: str, value: Any, ttl: int = None):
        """Fake cache put."""
        self.record_call("put", key, value, ttl)
        self.cached_items[key] = {"value": value, "ttl": ttl, "timestamp": datetime.now()}
        return True

    def get(self, key: str, default: Any = None):
        """Fake cache get."""
        self.record_call("get", key, default)
        item = self.cached_items.get(key)
        return item["value"] if item else default

    def forget(self, key: str):
        """Fake cache forget."""
        self.record_call("forget", key)
        if key in self.cached_items:
            del self.cached_items[key]
            return True
        return False

    def flush(self):
        """Fake cache flush."""
        self.record_call("flush")
        self.cached_items.clear()
        return True

    def has(self, key: str):
        """Fake cache has."""
        self.record_call("has", key)
        return key in self.cached_items

    def assert_cached(self, key: str, value: Any = None):
        """Assert that item was cached."""
        assert key in self.cached_items, f"Key {key} was not cached"

        if value is not None:
            cached_value = self.cached_items[key]["value"]
            assert cached_value == value, (
                f"Expected value '{value}', got '{cached_value}'"
            )

    def assert_not_cached(self, key: str):
        """Assert that item was not cached."""
        assert key not in self.cached_items, f"Key {key} was cached"

    def clear_cached(self):
        """Clear cached items."""
        self.cached_items.clear()
        self.clear_calls()
