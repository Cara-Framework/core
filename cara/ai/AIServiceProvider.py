"""Service provider that binds the default AI client to the container."""

from __future__ import annotations

from cara.ai.Client import AIClient
from cara.foundation import DeferredProvider


class AIServiceProvider(DeferredProvider):
    """Deferred provider for the AI subsystem.

    Binds ``ai`` to a default :class:`AIClient` (provider/model resolved from
    ``config("ai.*")``). Resolved lazily on first use of the ``AI`` facade.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["ai"]

    def register(self) -> None:
        self.application.bind("ai", AIClient())
