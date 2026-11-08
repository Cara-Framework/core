"""Logging — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CaraLoggerFactory": (".CaraLoggerFactory", "CaraLoggerFactory"),
    "CaraPythonLoggerAdapter": (".CaraPythonLoggerAdapter", "CaraPythonLoggerAdapter"),
    "CategoryFilter": (".CategoryFilter", "CategoryFilter"),
    "ChannelConfigurator": (".ChannelConfigurator", "ChannelConfigurator"),
    "ConsoleChannel": (".channels", "ConsoleChannel"),
    "ContextualLogger": (".ContextualLogger", "ContextualLogger"),
    "FileChannel": (".channels", "FileChannel"),
    "HttpColorizer": (".HttpColorizer", "HttpColorizer"),
    "Logger": (".Logger", "Logger"),
    "LoggerContract": (".contracts", "LoggerContract"),
    "LoggerProvider": (".LoggerProvider", "LoggerProvider"),
    "SiblingSafeRetention": (".SiblingSafeRetention", "SiblingSafeRetention"),
    "SlackChannel": (".channels", "SlackChannel"),
    "install_cara_loggers": (".PythonLoggerAdapter", "install_cara_loggers"),
    "per_process_sink": (".ChannelConfigurator", "per_process_sink"),
}

__all__ = [
    "CaraLoggerFactory",
    "CaraPythonLoggerAdapter",
    "CategoryFilter",
    "ChannelConfigurator",
    "ConsoleChannel",
    "ContextualLogger",
    "FileChannel",
    "HttpColorizer",
    "Logger",
    "LoggerContract",
    "LoggerProvider",
    "SiblingSafeRetention",
    "SlackChannel",
    "install_cara_loggers",
    "per_process_sink",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
