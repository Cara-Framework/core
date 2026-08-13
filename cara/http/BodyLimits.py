"""Single configuration authority for HTTP request-body resource limits."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from cara.configuration import config


@dataclass(frozen=True)
class BodyLimits:
    """Validated process limits shared by preflight and streaming parsers."""

    DEFAULT_BODY_BYTES: ClassVar[int] = 10 * 1024 * 1024
    DEFAULT_FILE_BYTES: ClassVar[int] = 10 * 1024 * 1024
    DEFAULT_FILES: ClassVar[int] = 20

    body_bytes: int
    file_bytes: int
    files: int

    @classmethod
    def configured(cls) -> BodyLimits:
        return cls(
            body_bytes=cls._positive(
                "server.max_body_size",
                config("server.max_body_size", cls.DEFAULT_BODY_BYTES),
            ),
            file_bytes=cls._positive(
                "server.max_file_size",
                config("server.max_file_size", cls.DEFAULT_FILE_BYTES),
            ),
            files=cls._positive(
                "server.max_files",
                config("server.max_files", cls.DEFAULT_FILES),
            ),
        )

    @staticmethod
    def _positive(key: str, value: object) -> int:
        if type(value) is not int or value <= 0:
            raise ValueError(f"{key} must be a positive integer")
        return value
