"""``QueueState`` broker topology and dead-letter vocabulary.

The dead-letter names below are framework vocabulary, not application
configuration. :class:`~cara.queues.drivers.AMQPDriver.AMQPDriver` stamps
``x-dead-letter-exchange`` / ``x-dead-letter-routing-key`` onto every canonical
queue it declares, and the replay rail in
:class:`~cara.queues.delivery.QueueJobDeliveryStore.QueueJobDeliveryStore`
expects the same trio to exist. They used to be spelled as bare literals inside
the driver *and* re-declared by hand in each application's reconcile command,
so one name had three homes and drifted between them.

``DEAD_LETTER_BINDING`` is ``dead.#`` rather than ``dead.*``: the driver's
dead-letter routing key is ``f"dead.{queue_name}"`` and a canonical queue name
may itself contain dots, so a single-word ``*`` match silently drops every
dead-letter for a dotted queue.

The declaration helpers implement the one protocol every reconcile needs —
declare the queue with the arguments the driver expects, and when the broker
refuses with a 406 (an existing queue whose arguments differ), replace it only
after proving it is empty and unused. A queue holding messages or consumers is
never deleted; the caller gets a :class:`~cara.exceptions.QueueException`
naming what blocked it.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any

try:
    # ``pika`` is the optional 'queue' extra (cara[queue]); mirror the driver's
    # degrade-to-None import so importing ``cara.queues`` never hard-requires it.
    import pika
except ImportError:  # pragma: no cover - exercised only without the extra
    pika = None  # type: ignore[assignment]

from cara.exceptions import QueueDriverLibraryNotFoundException, QueueException

DEAD_LETTER_EXCHANGE = "dead.letter.dlx"
DEAD_LETTER_QUEUE = "dead.letter.queue"
DEAD_LETTER_BINDING = "dead.#"

__all__ = [
    "DEAD_LETTER_BINDING",
    "DEAD_LETTER_EXCHANGE",
    "DEAD_LETTER_QUEUE",
    "QueueState",
    "close_quietly",
    "declare_dead_letter_topology",
    "ensure_exact_queue",
    "format_queue_states",
    "inspect_queue",
]


@dataclass(frozen=True)
class QueueState:
    """A passive snapshot of one broker queue."""

    name: str
    messages: int
    consumers: int

    @property
    def is_active(self) -> bool:
        """True when the queue holds work or has an attached consumer."""
        return self.messages > 0 or self.consumers > 0


def _require_pika() -> Any:
    if pika is None:  # pragma: no cover - exercised only without the extra
        raise QueueDriverLibraryNotFoundException(
            "Broker topology operations need the AMQP extra: "
            "pip install 'cara[queue]' (or add pika to the deployable)."
        )
    return pika


def close_quietly(handle: Any) -> None:
    """Close a pika connection or channel, tolerating an already-dead handle.

    Topology work opens a channel per declaration on purpose (a broker-closed
    channel poisons every later use of that same channel), so closing is the
    single most repeated operation in this module and must never mask the real
    failure with a teardown error.
    """
    if handle is None or not getattr(handle, "is_open", True):
        return
    suppressed: tuple[type[BaseException], ...] = (
        OSError,
        ConnectionError,
        RuntimeError,
        AttributeError,
    )
    if pika is not None:
        suppressed = (*suppressed, pika.exceptions.AMQPError)
    with contextlib.suppress(*suppressed):
        handle.close()


def inspect_queue(connection: Any, queue_name: str) -> QueueState | None:
    """Passively declare ``queue_name`` and report its depth, or ``None``.

    A passive declare against a missing queue answers 404 and closes the
    channel, which is why this opens (and closes) a channel of its own.
    """
    amqp = _require_pika()
    channel = connection.channel()
    try:
        result = channel.queue_declare(queue=queue_name, passive=True)
    except amqp.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code == 404:
            return None
        raise
    finally:
        close_quietly(channel)
    return QueueState(
        name=queue_name,
        messages=int(result.method.message_count),
        consumers=int(result.method.consumer_count),
    )


def ensure_exact_queue(
    connection: Any,
    queue_name: str,
    arguments: dict[str, Any],
) -> None:
    """Declare ``queue_name`` with exactly ``arguments``, replacing only if safe.

    The broker answers a declaration that disagrees with an existing queue's
    arguments with a 406 and closes the channel. That queue is replaced only
    when it is provably empty and unconsumed; otherwise this raises rather than
    discard queued work.
    """
    amqp = _require_pika()
    state = inspect_queue(connection, queue_name)
    declare_channel = connection.channel()
    try:
        declare_channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments=arguments,
        )
        return
    except amqp.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code != 406:
            raise
        if state is not None and state.is_active:
            raise QueueException(
                "Refusing to replace an incompatible active queue: "
                f"{format_queue_states([state])}"
            ) from exc
    finally:
        close_quietly(declare_channel)

    if state is not None:
        delete_channel = connection.channel()
        try:
            delete_channel.queue_delete(
                queue=queue_name,
                if_unused=True,
                if_empty=True,
            )
        except amqp.exceptions.ChannelClosedByBroker as exc:
            raise QueueException(
                f"Broker refused safe replacement of queue {queue_name!r}: "
                f"{exc.reply_text}"
            ) from exc
        finally:
            close_quietly(delete_channel)

    create_channel = connection.channel()
    try:
        create_channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments=arguments,
        )
    finally:
        close_quietly(create_channel)


def declare_dead_letter_topology(driver: Any, connection: Any, channel: Any) -> None:
    """Materialize the dead-letter exchange, queue and binding the driver expects.

    ``driver`` supplies the queue arguments (``dead_letter_queue_arguments``);
    every name comes from this module so the declaration can never disagree
    with what the driver stamps onto canonical queues.
    """
    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )
    ensure_exact_queue(
        connection,
        DEAD_LETTER_QUEUE,
        driver.dead_letter_queue_arguments(),
    )
    channel.queue_bind(
        queue=DEAD_LETTER_QUEUE,
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=DEAD_LETTER_BINDING,
    )


def format_queue_states(states: list[QueueState]) -> str:
    """Render queue states for an operator-facing refusal message."""
    return ", ".join(
        f"{state.name}(messages={state.messages}, consumers={state.consumers})"
        for state in states
    )
