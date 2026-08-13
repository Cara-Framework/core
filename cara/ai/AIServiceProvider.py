"""Service provider that binds the default AI client to the container."""

from __future__ import annotations

from cara.ai.AIClient import AIClient
from cara.configuration import config
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
        provider = config("ai.provider", "openrouter")
        provider_key = str(provider).lower()
        self.application.bind(
            "ai",
            AIClient(
                provider=provider,
                model=config(f"ai.{provider_key}_model", None),
                base_url=config(f"ai.{provider_key}_base_url", None),
                api_key=config(f"ai.{provider_key}_api_key", None),
                timeout=config("ai.timeout", 60),
                max_retries=config("ai.max_retries", 1),
                fallback_model=config("ai.fallback_model", None),
                site_url=config("ai.openrouter_site_url", None),
                site_name=config("ai.openrouter_site_name", None),
                default_temperature=config("ai.default_temperature", 0.3),
                default_max_tokens=config("ai.default_max_tokens", 1000),
                default_top_p=config("ai.default_top_p", 0.9),
                json_temperature=config("ai.json_temperature", 0.2),
                json_max_tokens=config("ai.json_max_tokens", 1500),
            ),
        )
