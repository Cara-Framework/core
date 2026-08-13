"""Canonical definition of ``LogType``."""

from __future__ import annotations

from enum import Enum


class LogType(Enum):
    """HTTP log types for different colorization styles."""

    HTTP = "http"
    ERROR = "error"
    WARNING = "warning"
    SUCCESS = "success"
    DEFAULT = "default"
