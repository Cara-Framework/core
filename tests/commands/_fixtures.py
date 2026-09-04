"""The command-under-test builder, owned once.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).

Every craft-command test needs the same three steps — construct without the
Typer wiring, install parsed options, silence the Rich console — and six files
had hand-written their own copy of them. One owner means a change to the
command base class is a change in one place.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock


def make_command[CommandT](
    command_class: type[CommandT],
    options: dict | None = None,
    application: Any = None,
    **attributes: Any,
) -> CommandT:
    """Build ``command_class`` ready to call, bypassing the Typer wiring.

    ``application`` stays ``None`` unless a test needs a real container (the
    command then resolves ``DB`` and friends from it). Extra keyword arguments
    are set as attributes, for the collaborators a command normally binds
    lazily inside ``handle()``.
    """
    command = command_class(application=application)
    command.set_parsed_options(options or {})
    command.console = MagicMock()
    for name, value in attributes.items():
        setattr(command, name, value)
    return command
