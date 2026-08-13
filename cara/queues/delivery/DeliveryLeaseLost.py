"""Execution settlement no longer owns the delivery lease."""

from cara.exceptions import QueueException


class DeliveryLeaseLost(QueueException):
    """Execution settlement no longer owns the delivery lease."""
