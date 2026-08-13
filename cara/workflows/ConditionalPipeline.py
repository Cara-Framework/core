"""Canonical definition of ``ConditionalPipeline``."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .Pipeline import Pipeline


class ConditionalPipeline:
    """Helper for conditional pipeline steps."""

    def __init__(self, pipeline: Pipeline, condition: Callable):
        self.pipeline = pipeline
        self.condition = condition

    def add(self, step_class, *args, **kwargs) -> Pipeline:
        """Add conditional step."""
        return self.pipeline.add(step_class, *args, condition=self.condition, **kwargs)
