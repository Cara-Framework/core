from .Configuration import Configuration
from .ConfigurationProvider import ConfigurationProvider

__all__ = [
    "Configuration",
    "ConfigurationProvider",
    "config",
]


def config(key, default=None):
    """
    Retrieve a configuration value by dot‐notation key.

    If no Configuration instance exists yet, create it (but do NOT auto‐load). The actual `.load()`
    is performed in ConfigurationProvider.boot().
    """
    if not Configuration._instance:
        # Create a “bare” singleton so that future calls to config() won’t break.
        # Note: we do NOT call .load() here. That happens during ConfigurationProvider.boot().
        Configuration()
    return Configuration._instance.get(key, default)
