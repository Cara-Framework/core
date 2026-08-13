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
    "parse_json": (".Parsing", "parse_json"),
}

__all__ = [
    "AIClient",
    "AIConfigurationError",
    "AIException",
    "AIProvider",
    "AIResponse",
    "AIResponseError",
    "AIServiceProvider",
    "parse_json",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
