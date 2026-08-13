"""Persisted outbox metadata and its signed envelope disagree."""

from cara.exceptions import QueueException


class DeliveryEnvelopeMismatch(QueueException):
    """Persisted outbox metadata and its signed envelope disagree."""
