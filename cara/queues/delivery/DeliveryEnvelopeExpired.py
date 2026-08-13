"""A valid immutable envelope expired before broker publication."""

from cara.exceptions import QueueException


class DeliveryEnvelopeExpired(QueueException):
    """A valid immutable envelope expired before broker publication."""
