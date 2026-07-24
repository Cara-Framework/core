from __future__ import annotations

import importlib
import subprocess
import sys


def test_leaf_command_import_does_not_eager_load_watchdog() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import cara.commands.BootlessCommandRunner; "
            "assert not any(n == 'watchdog' or n.startswith('watchdog.') "
            "for n in sys.modules)",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_package_exports_preserve_public_api() -> None:
    commands = importlib.import_module("cara.commands")

    assert commands.CommandBase.__name__ == "CommandBase"
    assert commands.missing_optional.__name__ == "missing_optional"
