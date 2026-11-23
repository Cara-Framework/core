"""``job.delay()`` is callable, and ``queue_name`` reaches ``job.queue``.

Live defects in ``cara/queues/contracts``:

1. ``Queueable.__init__`` set an instance attribute ``self.delay = None``,
   which shadowed ``BaseQueueable.delay()`` (and ``BaseNotification.delay()``),
   so ``job.delay(30)`` raised ``TypeError: 'NoneType' object is not
   callable``. The delay is now kept on ``_delay``.
2. ``BaseQueueable`` kept ``default_queue`` / ``on_queue()`` on
   ``queue_name`` while the driver and ``PendingDispatch`` read ``job.queue``,
   so every such job was refused for declaring no canonical queue. Setting
   ``queue_name`` now mirrors onto ``queue``.
"""

from __future__ import annotations

from cara.queues.contracts import BaseJob


class _MailLikeJob(BaseJob):
    default_queue = "emails"

    async def handle(self):
        return None


class _NoDefaultJob(BaseJob):
    async def handle(self):
        return None


def test_delay_is_a_method_not_shadowed_by_an_attribute() -> None:
    job = _MailLikeJob()

    assert job.delay(30) is job
    assert job._delay == 30
    assert callable(job.delay)


def test_default_queue_reaches_the_attribute_the_driver_reads() -> None:
    assert _MailLikeJob().queue == "emails"
    assert _NoDefaultJob().queue is None


def test_on_queue_reaches_the_attribute_the_driver_reads() -> None:
    job = _MailLikeJob().on_queue("priority-mail")

    assert job.queue == "priority-mail"
    assert job.queue_name == "priority-mail"


def test_setting_queue_name_directly_also_moves_queue() -> None:
    job = _NoDefaultJob()
    job.queue_name = "reports"

    assert job.queue == "reports"
