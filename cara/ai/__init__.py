"""Cara AI subsystem — provider-agnostic LLM client + robust JSON parsing."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AIClient": (".AIClient", "AIClient"),
    "AIConfigurationError": (".AIConfigurationError", "AIConfigurationError"),
    "AIException": (".AIException", "AIException"),
    "AIProvider": (".AIProvider", "AIProvider"),
    "AIResponse": (".AIResponse", "AIResponse"),
    "AIResponseError": (".AIResponseError", "AIResponseError"),
    "AIServiceProvider": (".AIServiceProvider", "AIServiceProvider"),
    "AUTH_HOLD_SECONDS": (".ProviderHealth", "AUTH_HOLD_SECONDS"),
    "DATA_RULES": (".PromptSafety", "DATA_RULES"),
    "OUTAGE_HOLD_SECONDS": (".ProviderHealth", "OUTAGE_HOLD_SECONDS"),
    "ProviderFailure": (".ProviderHealth", "ProviderFailure"),
    "answer_fields": (".PromptSafety", "answer_fields"),
    "answer_text": (".PromptSafety", "answer_text"),
    "data_block": (".PromptSafety", "data_block"),
    "parse_json": (".Parsing", "parse_json"),
    "provider_failure_kind": (".ProviderHealth", "provider_failure_kind"),
    "provider_unhealthy": (".ProviderHealth", "provider_unhealthy"),
    "record_provider_failure": (".ProviderHealth", "record_provider_failure"),
    "record_provider_success": (".ProviderHealth", "record_provider_success"),
    "untrusted": (".PromptSafety", "untrusted"),
    "untrusted_text": (".PromptSafety", "untrusted_text"),
}

__all__ = [
    "AIClient",
    "AIConfigurationError",
    "AIException",
    "AIProvider",
    "AIResponse",
    "AIResponseError",
    "AIServiceProvider",
    "AUTH_HOLD_SECONDS",
    "DATA_RULES",
    "OUTAGE_HOLD_SECONDS",
    "ProviderFailure",
    "answer_fields",
    "answer_text",
    "data_block",
    "parse_json",
    "provider_failure_kind",
    "provider_unhealthy",
    "record_provider_failure",
    "record_provider_success",
    "untrusted",
    "untrusted_text",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
