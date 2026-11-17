r"""Reading what a craft command PRINTED, not how rich painted it.

Typer draws command output through rich, and when ``GITHUB_ACTIONS`` is set it
builds that console with ``force_terminal=True`` (``typer/rich_utils.py``), so
rich emits SGR escapes even though pytest's capture is not a terminal. That one
environment variable is the whole difference between a developer's run and a
CI run of the same command.

The coloring SPLITS an option name in two. ``OptionHighlighter`` carries both
``(^|\W)(?P<switch>\-\w+)`` and ``(^|\W)(?P<option>\-\-[\w\-]+)``. Against
``--port`` the switch pattern spends the first dash on its ``\W`` prefix and
spans ``-port``, while the option pattern spans ``--port``. The two spans begin
one character apart, so rich writes
``\x1b[1;36m-\x1b[0m\x1b[1;36m-port\x1b[0m``: the literal ``--port`` is ABSENT
from the capture, and a bare ``assert "--port" in output`` fails against help
text that does list the flag.

It is neither a width nor a dispatch problem: stripping the escapes passes at
the 80 columns CI renders, and pinning ``COLUMNS`` on the raw capture does not.
"""

from __future__ import annotations

import re

#: Select Graphic Rendition escapes — the only escapes rich emits here.
_SGR = re.compile(r"\x1b\[[0-9;]*m")


def strip_sgr(captured: str) -> str:
    """``captured`` with every SGR escape removed."""
    return _SGR.sub("", captured)


def assert_cli_prints(captured: str, expected: str, command: str) -> str:
    r"""Return the de-escaped capture, failing with ALL of it when EXPECTED is absent.

    pytest elides the raw capture's ``repr`` once escapes inflate it, which is
    how this failure first reached CI unreadable — ``assert '--port' in
    '\x1b[1m ... \x1b[0m\n\x1b[1...'``. Raising with the stripped text keeps the
    next failure diagnosable from the log alone.
    """
    plain = strip_sgr(captured)
    if expected not in plain:
        raise AssertionError(
            f"`craft {command}` did not print {expected!r}.\n"
            f"--- capture, SGR stripped ({len(plain)} chars) ---\n"
            f"{plain}\n"
            f"--- end of capture ---\n"
            f"raw head (repr, 400 chars): {captured[:400]!r}"
        )
    return plain
