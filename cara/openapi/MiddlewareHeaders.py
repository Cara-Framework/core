"""Canonical definition of ``MiddlewareHeaders``."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .InvalidHeaderComponent import InvalidHeaderComponent


@dataclass(frozen=True)
class MiddlewareHeaders:
    """The response headers one middleware stamps on the operations it guards.

    ``components`` holds the OpenAPI header objects, published once under
    ``components.headers``. ``passed`` names the headers on every response the
    operation itself returns through the middleware; ``refused`` names, per
    status, the headers on the middleware's own refusal. Nothing else carries
    them: a refusal from a middleware that ran first never reached this one,
    and an error raised past it — a failed validation — leaves through the
    exception handler without them, so each is documented without them.
    """

    components: Mapping[str, Mapping[str, Any]]
    passed: tuple[str, ...] = ()
    refused: Mapping[int, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        named = {
            *self.passed,
            *(header for headers in self.refused.values() for header in headers),
        }
        undefined = sorted(named - set(self.components))
        if undefined:
            raise InvalidHeaderComponent(
                "MiddlewareHeaders names header(s) it does not define: "
                + ", ".join(undefined)
            )
