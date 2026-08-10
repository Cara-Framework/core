"""Process-local canonical queue routing."""

from .QueueRouter import (
    ONE_WORD,
    QueueRouter,
    RoutingKey,
    matches_pattern,
    patterns_overlap,
)

__all__ = [
    "ONE_WORD",
    "QueueRouter",
    "RoutingKey",
    "matches_pattern",
    "patterns_overlap",
]
