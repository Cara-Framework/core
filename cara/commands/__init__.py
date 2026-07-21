from .MakesAutoReload import MakesAutoReload
from .Command import Command
from .CommandBase import CommandBase
from .CommandLoader import CommandLoader
from .CommandRegistry import CommandRegistry
from .CommandRunner import CommandRunner
from .CommandProvider import CommandProvider
from ._optional import OptionalDependencyError, missing_optional

__all__ = [
    "Command",
    "CommandBase",
    "CommandLoader",
    "CommandProvider",
    "CommandRegistry",
    "CommandRunner",
    "MakesAutoReload",
    "OptionalDependencyError",
    "missing_optional",
    "run_build_command",
]


def run_build_command(argv: list[str]) -> int | None:
    """Run a ``build:*`` command WITHOUT booting the application.

    Build commands are pure filesystem steps used inside a Docker build stage,
    where none of the production secrets exist, so they must not trigger the
    config/provider boot that ``bootstrap`` performs. The ``craft`` entry
    dispatches here BEFORE importing bootstrap; returns the command's exit code,
    or ``None`` when *argv* is an ordinary command (the caller then boots and
    runs it normally).

    Build commands are ordinary ``@command``-registered ``CommandBase`` classes
    (e.g. ``build:vendor-commons``); this just instantiates the matching one with
    no application and calls ``handle()`` directly, bypassing the CLI's Typer
    dispatch (which the framework boot builds).
    """
    if not argv or not argv[0].startswith("build:"):
        return None

    import sys

    # importing the built-in build command(s) registers them via @command
    from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand  # noqa: F401
    from cara.decorators import get_registered_commands

    for cmd_cls in get_registered_commands():
        if getattr(cmd_cls, "name", None) == argv[0]:
            result = cmd_cls(application=None).handle()
            return result if isinstance(result, int) else 0

    print(f"cara: unknown build command {argv[0]!r}", file=sys.stderr)
    return 2
