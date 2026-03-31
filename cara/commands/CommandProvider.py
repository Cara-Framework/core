"""
Command Provider for the Cara framework.

This module provides the service provider that registers CLI commands into the application.
"""

from typing import Any

from cara.commands import Command
from cara.facades import Config
from cara.foundation import Provider


class CommandProvider(Provider):
    """
    Binds only:
      - Command (the entire CLI) under 'commands'
    No discovery or registration logic here.
    """

    @classmethod
    def provides(cls):
        return ["commands"]

    def __init__(self, application: Any):
        self.application = application

    def register(self) -> None:
        self.application.bind(
            "commands",
            Command(
                self.application,
                watch=bool(
                    Config.get("application.debug", False),
                ),
            ),
        )
