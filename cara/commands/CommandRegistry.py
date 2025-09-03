"""
Command Registry for the Cara framework.

This module provides a registry for managing and discovering CLI commands in the application.
"""

from cara.decorators import get_registered_commands


class CommandRegistry:
    """Registers and lists command classes."""

    @staticmethod
    def get():
        return get_registered_commands()

    @staticmethod
    def clear():
        get_registered_commands().clear()
