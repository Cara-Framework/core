"""CLI entry point for the Cara framework.

Referenced by ``setup.py`` console_scripts as ``cara=cara.commands.Cli:main``.
"""

from __future__ import annotations


def main() -> None:
    from cara.foundation import Application

    app = Application()
    app.register_commands()
    app.run_console()


if __name__ == "__main__":
    main()
