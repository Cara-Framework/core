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
        # ``EnforceBodySizeLimit`` asks on every request. The three limits
        # live in one ``server`` section, so it is fetched once and read as a
        # dict instead of walking the configuration tree three times; a reader
        # that does not return the section (a test double keyed by full
        # paths) is asked limit by limit.
        section = config("server", None)
        if isinstance(section, dict):

            def read(name: str, default: int) -> object:
                return section.get(name, default)

        else:

            def read(name: str, default: int) -> object:
                return config(f"server.{name}", default)

        return cls(
            body_bytes=cls._positive(
                "server.max_body_size",
                read("max_body_size", cls.DEFAULT_BODY_BYTES),
            ),
            file_bytes=cls._positive(
                "server.max_file_size",
                read("max_file_size", cls.DEFAULT_FILE_BYTES),
            ),
            files=cls._positive(
                "server.max_files",
                read("max_files", cls.DEFAULT_FILES),
            ),
        )

    @staticmethod
    def _positive(key: str, value: object) -> int:
        if type(value) is not int or value <= 0:
            raise ValueError(f"{key} must be a positive integer")
        return value
