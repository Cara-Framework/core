"""
Queue job for sending mailables in background.

Created by ``Mail.send`` for a ``ShouldQueue`` mailable and by
``MailMessage.queue()``.
"""

from __future__ import annotations

from typing import Any

from cara.context import ExecutionContext
from cara.exceptions import CaraException
from cara.facades import Mail
from cara.mail.Mailable import Mailable
from cara.queues import QueuePayload
from cara.queues.contracts import BaseJob


class SendMailableJob(BaseJob):
    """
    Job to send one built mailable in the background.

    The job carries PLAIN DATA only — the mailable's fully qualified class
    path and the message its ``to_queue_payload()`` produced — and rebuilds
    the mailable on the worker through ``from_queue_payload()``. It used to
    carry the live ``Mailable``, which the signed JSON envelope refuses by
    design, so no queued mail was ever dispatched.
    """

    # Mail-specific queue settings
    default_queue = "emails"
    # Mail needs more retries. Read by the worker through ``max_attempts``
    # (``BaseQueueable``); declared here and never read, queued mail got the
    # worker's default 3.
    default_retry_attempts = 5

    def __init__(
        self,
        mailable_class: str,
        mailable_data: dict[str, Any],
        driver_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            mailable_class: Fully qualified class path of the mailable.
            mailable_data: The mailable's ``to_queue_payload()``.
            driver_name: Optional mail driver name.
        """
        self.mailable_class = mailable_class
        self.mailable_data = dict(mailable_data or {})
        self.driver_name = driver_name
        # ``payload`` is derived from the fields above. The worker rebuilds
        # this job with every constructor-chain parameter, BaseJob's
        # ``payload`` included, and forwarding it as well raised "got
        # multiple values for keyword argument 'payload'".
        kwargs.pop("payload", None)
        super().__init__(payload={"mailable_type": mailable_class}, **kwargs)

    @classmethod
    def for_mailable(
        cls,
        mailable: Mailable,
        driver_name: str | None = None,
        *,
        application: Any = None,
    ) -> SendMailableJob:
        """Build the job for ``mailable``: build and render it HERE, at dispatch.

        Rendering on the dispatching side is what lets the job carry plain
        data: the worker receives the finished message, not the view data
        (which commonly holds models). Anything that is not plain data is
        refused now, with the field named, before anything is queued.
        """
        if not isinstance(mailable, Mailable):
            raise CaraException(
                f"Only a Mailable can be queued, got {type(mailable).__name__}."
            )
        mailable.set_application(application)
        mailable.build()
        try:
            data = mailable.to_queue_payload()
        finally:
            # The application is a live container; it must not stay on an
            # object that may be inspected or reused after dispatch.
            mailable.set_application(None)
        # Refused HERE when the worker would refuse to import the class
        # (``QueuePayload.resolve_class``), not as a job that dead-letters.
        mailable_class = QueuePayload.require_trusted(
            QueuePayload.class_path(type(mailable))
        )
        job = cls(mailable_class, data, driver_name)
        queue = mailable.get_queue()
        if queue:
            job.on_queue(queue)
        return job

    async def handle(self):
        """
        Rebuild the mailable and send it.

        ``handle`` MUST be async: ``SignedJsonJobSerializer`` rejects a
        sync ``handle`` at both serialize and deserialize.

        ``Mail._send_now`` is blocking SMTP socket I/O, so it crosses the
        boundary through ``ExecutionContext.run_in_thread`` (which copies
        the contextvar snapshot — job id, correlation id, tenancy — and
        gives the thread its own DB connection registry) rather than
        stalling the worker's event loop.
        """
        mailable_cls = QueuePayload.resolve_class(self.mailable_class, Mailable)
        mailable = mailable_cls.from_queue_payload(self.mailable_data)

        result = await ExecutionContext.run_in_thread(
            Mail._send_now,
            mailable,
            self.driver_name,
        )

        if not result:
            # A driver answers a bool, and ``False`` includes "maybe
            # delivered" (SMTP lost after DATA) next to "certainly not".
            # A retry could send the mail twice, so the job is not retried:
            # it dead-letters, visible to operators, instead. Transient
            # failures are already retried inside the driver's own ladder,
            # and an exception raised by the driver still retries normally.
            error = CaraException(
                f"Mail driver did not confirm delivery of {self.mailable_class}; "
                "not retried, as the message may already have been delivered."
            )
            error.do_not_retry = True
            raise error
