"""Configuration — layer barrel (generated, DOCTRINE §5.1)."""

from cara.exceptions import InvalidConfigurationSetupException

from cara._LazyExports import _install_lazy_exports

from .Configuration import Configuration


def config(key, default=None):
    """Retrieve a loaded configuration value by dot-notation key.

    Access before ``ConfigurationProvider`` registers the application is a
    lifecycle error. Returning values from an invented empty singleton made
    required configuration indistinguishable from a missing optional key.
    """
    if Configuration._instance is None:
        raise InvalidConfigurationSetupException(
            "Configuration is unavailable before ConfigurationProvider registration"
        )
    return Configuration._instance.get(key, default)


_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ConfigurationProvider": (".ConfigurationProvider", "ConfigurationProvider"),
}

__all__ = [
    "Configuration",
    "ConfigurationProvider",
    "config",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
