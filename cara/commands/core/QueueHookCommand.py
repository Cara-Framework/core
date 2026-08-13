"""Canonical ``QueueHookCommand`` command."""

from __future__ import annotations

import os
import uuid

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.exceptions import InvalidArgumentException
from cara.facades import Queue


@command(
    name="queue:hook",
    help="Process one claimed terminal-hook row in an isolated process.",
    options=[
        {
            "name": "--job-id",
            "help": "Queue delivery UUID.",
            "type": str,
            "default": None,
            "is_flag": False,
        },
    ],
)
class QueueHookCommand(CommandBase):
    """Internal single-hook subprocess target."""

    def handle(self, job_id: str | None = None) -> int:
        try:
            canonical = str(uuid.UUID(str(job_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise InvalidArgumentException("--job-id must be a valid UUID.") from exc
        processed = Queue.driver("amqp").process_terminal_hook(canonical)
        return 0 if processed else getattr(os, "EX_TEMPFAIL", 75)
