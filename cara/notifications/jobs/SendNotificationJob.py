"""
Queue job for sending notifications in background.

Created by the Notification manager for a ``ShouldQueue`` notification: one
job per channel.
"""

from __future__ import annotations

from typing import Any

from cara.context import ExecutionContext
from cara.exceptions import CaraException, InvalidArgumentException
from cara.facades import Notification
from cara.notifications.BaseNotification import BaseNotification
from cara.notifications.contracts import NotifiableContract
from cara.queues import QueuePayload
from cara.queues.contracts import BaseJob


class SendNotificationJob(BaseJob):
    """
    Job to deliver one notification on the channels it names.

    The job carries PLAIN DATA only: the fully qualified class path and the
    ``to_queue_payload()`` of both the notifiable and the notification, and
    rebuilds each on the worker through its ``from_queue_payload()``. It used
    to carry the live objects, which the signed JSON envelope refuses by
    design, so no queued notification was ever dispatched.

    ``channels`` is what makes a retry safe. A retry replays the SAME signed
    envelope, so a job cannot record "mail already went out" for its next
    attempt; the manager therefore queues one job per channel, and a retry
    re-attempts only the channel whose job failed — never a sibling channel
    that had already delivered.
    """

    def __init__(
        self,
        notifiable_class: str,
        notifiable_data: dict[str, Any],
        notification_class: str,
        notification_data: dict[str, Any],
        channels: list[str],
        **kwargs: Any,
    ) -> None:
        self.notifiable_class = notifiable_class
        self.notifiable_data = dict(notifiable_data or {})
        self.notification_class = notification_class
        self.notification_data = dict(notification_data or {})
        self.channels = [str(name) for name in channels or ()]
        if not self.channels:
            raise InvalidArgumentException(
                "SendNotificationJob needs at least one channel to deliver on."
            )
        # ``payload`` is derived from the fields above. The worker rebuilds
        # this job with every constructor-chain parameter, BaseJob's
        # ``payload`` included, and forwarding it as well raised "got
        # multiple values for keyword argument 'payload'".
        kwargs.pop("payload", None)
        super().__init__(
            payload={
                "notification_type": notification_class,
                "channels": list(self.channels),
            },
            **kwargs,
        )

    @staticmethod
    def payload_of(subject: Any, role: str) -> tuple[str, dict[str, Any]]:
        """``(class path, plain data)`` for a notifiable or notification.

        Refused at dispatch, naming ``role``, when the object does not
        implement the queue payload contract.
        """
        to_payload = getattr(subject, "to_queue_payload", None)
        if not callable(to_payload) or not callable(
            getattr(type(subject), "from_queue_payload", None)
        ):
            raise InvalidArgumentException(
                f"{role} {type(subject).__name__} cannot be queued: it must implement "
                "to_queue_payload() and the classmethod from_queue_payload()."
            )
        contract = SendNotificationJob._CONTRACTS.get(role)
        if contract is not None and not isinstance(subject, contract):
            raise InvalidArgumentException(
                f"{role} {type(subject).__name__} cannot be queued: it must be a "
                f"{contract.__name__}."
            )
        return (
            # Refused HERE, at dispatch, when the worker would refuse to
            # import it — not as a job that dead-letters later.
            QueuePayload.require_trusted(QueuePayload.class_path(type(subject))),
            QueuePayload.require_plain(to_payload(), f"{role}_data"),
        )

    #: The contract each rebuilt object must implement. Resolving against
    #: ``object`` accepted ANY importable class with a callable
    #: ``from_queue_payload``.
    _CONTRACTS: dict[str, type] = {
        "notifiable": NotifiableContract,
        "notification": BaseNotification,
    }

    @staticmethod
    def _rebuild(class_path: str, data: dict[str, Any], role: str) -> Any:
        rebuilt_cls = QueuePayload.resolve_class(
            class_path, SendNotificationJob._CONTRACTS[role]
        )
        from_payload = getattr(rebuilt_cls, "from_queue_payload", None)
        if not callable(from_payload):
            raise CaraException(f"Queued class {class_path} has no from_queue_payload().")
        return from_payload(data)

    async def handle(self):
        """
        Rebuild notifiable and notification, then deliver on ``channels``.

        ``handle`` MUST be async: ``SignedJsonJobSerializer`` rejects a
        sync ``handle`` at both serialize and deserialize.

        ``Notification._send_now`` fans out over blocking channel drivers
        (SMTP, HTTP), so it crosses the boundary through
        ``ExecutionContext.run_in_thread`` — which copies the contextvar
        snapshot and gives the thread its own DB connection registry —
        rather than stalling the worker's event loop.
        """
        notifiable = self._rebuild(
            self.notifiable_class, self.notifiable_data, "notifiable"
        )
        notification = self._rebuild(
            self.notification_class, self.notification_data, "notification"
        )

        result = await ExecutionContext.run_in_thread(
            Notification._send_now,
            notifiable,
            notification,
            only_channels=list(self.channels),
        )

        if result is Notification.NOTHING_DUE:
            # ``should_send`` skipped the channel: done, not failed. Raising
            # here retried a notification that had deliberately been skipped.
            return None
        if not result:
            # A channel answered False rather than raising. For mail that
            # includes "maybe delivered" (SMTP lost after DATA), so a retry
            # could deliver twice: the job dead-letters, visible to
            # operators, instead. A channel that RAISES still retries.
            error = CaraException(
                f"Notification channel(s) {', '.join(self.channels)} did not "
                f"confirm delivery of {self.notification_class}; not retried, as "
                "it may already have been delivered."
            )
            error.do_not_retry = True
            raise error
        return None
