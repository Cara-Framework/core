"""Run explicitly allowlisted CLI commands without booting an application."""

from __future__ import annotations

import importlib.util
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cara.commands.CommandBase import CommandBase
from cara.commands.CommandRunner import CommandRunner


@dataclass(frozen=True, slots=True)
class BootlessCommandSpec:
    """Filesystem location and public identity of a bootless command."""

    name: str
    path: Path
    class_name: str


class _DirectApplication:
    """Minimal invocation seam for commands that have no container dependencies."""

    @staticmethod
    def call(callback: Any, *args: Any, **kwargs: Any) -> Any:
        """Invoke both plain and decorator-wrapped command callbacks.

        Command decorators bind their command instance positionally before
        forwarding parsed CLI kwargs. A bootless runner has no container, but
        it must preserve that normal callback contract.
        """
        return callback(*args, **kwargs)


def _load_command(spec: BootlessCommandSpec) -> type[CommandBase]:
    path = spec.path.resolve()
    module_spec = importlib.util.spec_from_file_location(
        f"_cara_bootless_{path.stem}",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise ImportError(f"Cannot load bootless command module: {path}")

    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    command_class = getattr(module, spec.class_name, None)
    if not isinstance(command_class, type) or not issubclass(
        command_class,
        CommandBase,
    ):
        raise TypeError(f"{spec.class_name} in {path} must extend CommandBase")
    if getattr(command_class, "name", None) != spec.name:
        raise ValueError(
            f"Bootless command spec names {spec.name!r}, but "
            f"{spec.class_name}.name is {getattr(command_class, 'name', None)!r}"
        )
    return command_class


def dispatch_bootless(
    argv: list[str],
    specs: tuple[BootlessCommandSpec, ...],
) -> bool:
    """Run a matching bootless command and return whether it was dispatched."""
    if not argv:
        return False

    command_names = {spec.name for spec in specs}
    if argv[0] not in command_names:
        return False

    runner = CommandRunner(
        _DirectApplication(),
        instrument_commands=False,
    )

    # Force Typer group mode even when a product exposes only one bootless
    # command. The original ``craft <command>`` argv shape then stays intact.
    @runner.console_app.callback()
    def _root() -> None:
        return None

    for spec in specs:
        command_class = _load_command(spec)
        signature = inspect.signature(command_class.handle)
        _, dependency_parameters = runner._split_handle_params(
            signature,
            command_class.handle,
        )
        if dependency_parameters:
            names = ", ".join(parameter.name for parameter in dependency_parameters)
            raise TypeError(
                f"Bootless command {spec.name!r} cannot use container dependencies: "
                f"{names}"
            )
        runner.register(command_class)

    runner.run()
    return True


_CORE_COMMANDS = Path(__file__).resolve().parent / "core"
_ARCHITECTURE_SPECS = (
    BootlessCommandSpec(
        name="arch:barrels",
        path=_CORE_COMMANDS / "ArchBarrelsCommand.py",
        class_name="ArchBarrelsCommand",
    ),
    BootlessCommandSpec(
        name="arch:check",
        path=_CORE_COMMANDS / "ArchCheckCommand.py",
        class_name="ArchCheckCommand",
    ),
)


def dispatch_architecture(argv: list[str]) -> bool:
    """Dispatch Cara's canonical architecture commands without app bootstrap."""
    return dispatch_bootless(argv, _ARCHITECTURE_SPECS)


__all__ = ["BootlessCommandSpec", "dispatch_architecture", "dispatch_bootless"]
