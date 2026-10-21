"""Table-to-model resolution for the database-backed validation rules.

A rule names a table (``exists:product,id``); ``exists`` and ``unique`` query
it through the model resolved here so the model's scopes (tenancy, soft
deletes) apply, and ``batch_exists`` takes its physical table name from it.
"""

from __future__ import annotations

import importlib

from cara.exceptions import ConfigurationException
from cara.support import ModuleManager


def _resolve_rule_model(table_name: str) -> type:
    """Resolve one table through the configured, generated model barrel.

    The model barrel is the canonical registry: every public model is
    exported there and each model owns an explicit ``__table__``. Missing
    or ambiguous registrations are configuration errors. Raw SQL fallback
    is forbidden because it would bypass model scopes such as tenancy.
    """
    models_module = ModuleManager.models_module()
    if not isinstance(models_module, str) or not models_module:
        raise ConfigurationException("Models module is not configured")
    try:
        model_barrel = importlib.import_module(models_module)
    except ImportError as exc:
        raise ConfigurationException(
            f"Configured models module '{models_module}' is not importable"
        ) from exc

    matches = [
        candidate
        for candidate in vars(model_barrel).values()
        if isinstance(candidate, type)
        and getattr(candidate, "__table__", None) == table_name
    ]
    if len(matches) != 1:
        raise ConfigurationException(
            f"Table '{table_name}' must resolve to exactly one model in "
            f"'{models_module}'; found {len(matches)}"
        )
    return matches[0]
