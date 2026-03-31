"""
Console Output Utilities Module.

This module provides console output utilities for the Cara framework, implementing colored output
and command-line formatting with support for different message types and styles.
"""


class HasColoredOutput:
    """
    Base class for colored console output.

    This class provides methods for outputting colored messages to the console, with support for
    success, warning, danger, and info message types.
    """

    def success(self, message):
        print("\033[92m {0} \033[0m".format(message))

    def warning(self, message):
        print("\033[93m {0} \033[0m".format(message))

    def danger(self, message):
        print("\033[91m {0} \033[0m".format(message))

    def info(self, message):
        return self.success(message)


class AddCommandColors:
    """
    Command-line color formatting.

    This class provides methods for formatting command-line output with colors, specifically focused
    on error and warning messages in CLI commands.
    """

    def error(self, text):
        """
        Write a string as information output.

        :param text: The line to write
        :type text: str
        """
        self.line(text, "error")

    def warning(self, text):
        """
        Write a string as information output.

        :param text: The line to write
        :type text: str
        """
        self.line(text, "c2")
