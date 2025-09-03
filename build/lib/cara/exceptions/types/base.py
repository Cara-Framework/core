"""Base exception classes for the Cara framework."""


class CaraException(Exception):
    """Base class for all Cara framework exceptions."""

    def __init__(self, message: str = "An error occurred"):
        super().__init__(message)
