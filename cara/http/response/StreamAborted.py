"""The signal that a streamed response cannot be finished."""

from __future__ import annotations


class StreamAborted(BaseException):
    """A stream failed after its headers went out.

    The status cannot change any more, and an exception handler can only end
    the body cleanly, which reads as a complete file. A ``BaseException`` so
    no ``except Exception`` layer turns it into that: it reaches the ASGI
    server, which closes the connection with no end frame."""


__all__ = ["StreamAborted"]
