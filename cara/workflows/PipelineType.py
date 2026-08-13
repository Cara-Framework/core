"""Canonical definition of ``PipelineType``."""

from __future__ import annotations

from enum import Enum


class PipelineType(Enum):
    """Pipeline execution types."""

    SYNC = "sync"  # Execute immediately (commands)
