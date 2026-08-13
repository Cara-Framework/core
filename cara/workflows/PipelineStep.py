"""Canonical definition of ``PipelineStep``."""

from __future__ import annotations

from collections.abc import Callable


class PipelineStep:
    """Individual step in a pipeline."""

    def __init__(
        self,
        step_class,
        args: tuple = (),
        kwargs: dict | None = None,
        condition: Callable | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
    ):
        """
        Initialize pipeline step.

        Args:
            step_class: Command or Job class to execute
            args: Arguments to pass to step
            kwargs: Keyword arguments to pass to step
            condition: Optional condition function to determine if step should run
            on_success: Callback on step success
            on_failure: Callback on step failure
        """
        self.step_class = step_class
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.condition = condition
        self.on_success = on_success
        self.on_failure = on_failure
