"""
Interface marker for queueable classes.

This module defines a marker interface that indicates a class should be processed through the queue
system rather than synchronously.
"""

from __future__ import annotations


class ShouldQueue:
    #: What a retry of this job carries over from the run that failed.
    #:
    #: The rule: a retry is the work that was dispatched. It is rebuilt from
    #: the signed ``init_kwargs`` of the failed delivery, so whatever the
    #: failed run did to its constructor attributes is undone — a run that
    #: trimmed a list, advanced a cursor or rewrote a field incidentally
    #: would otherwise ship a DIFFERENT job under the original's identity
    #: (and, where the idempotency key is derived from those attributes, a
    #: different key, so the retry no longer deduplicated against its own
    #: first run).
    #:
    #: A job that means to resume from progress names those constructor
    #: parameters (or stored ``**kwargs`` entries) here, e.g. an adopted run
    #: id: ``retry_carries = ("run_id",)``. Only they keep the value the
    #: failed run gave them. ``()`` (the default) carries nothing.
    retry_carries: tuple[str, ...] = ()

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        carries = cls.__dict__.get("retry_carries", ())
        if isinstance(carries, str):
            # ``("run_id")`` is a str, not a tuple: carry the one name rather
            # than every character of it.
            carries = (carries,)
        if not isinstance(carries, (tuple, list)) or not all(
            isinstance(name, str) and name for name in carries
        ):
            # Refused at import, never mid-retry: a malformed declaration
            # found in the failure path could only cost the retry itself.
            raise TypeError(
                f"{cls.__name__}.retry_carries must be a tuple of "
                f"constructor-parameter names, not {carries!r}."
            )
        if "retry_carries" in cls.__dict__:
            cls.retry_carries = tuple(carries)
