"""
Facade pattern implementation for Cara framework.

Provides a clean way to access services from the container as static-like methods.
Similar to Laravel facades - services are resolved on first access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cara.exceptions import CaraException

if TYPE_CHECKING:
    from cara.logging import Logger


class Facade(type):
    """Metaclass for creating service facades.

    Allows static-like access to container-resolved services with automatic
    dependency injection and error handling.
    """

    key: str = ""  # Must be set by subclasses to the container key

    def __getattr__(cls, attribute: str) -> Any:
        """Resolve attribute from container service.

        Args:
            attribute: The attribute/method name to resolve

        Returns:
            The attribute from the resolved service

        Raises:
            CaraException: If bootstrap is unavailable and no fallback exists
            AttributeError: If service cannot be resolved or attribute doesn't exist
        """
        try:
            import builtins

            application = builtins.app()
        except (ImportError, ModuleNotFoundError, TypeError, AttributeError):
            # Handle application-not-booted with targeted fallbacks
            # (e.g. running stress tests outside the full Cara framework,
            # or Python version mismatch causing TypeError on 3.10+ syntax)
            if cls.key == "logger":
                import logging

                _fallback = logging.getLogger("cara.fallback")
                if not _fallback.handlers:
                    _h = logging.StreamHandler()
                    _h.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
                    _fallback.addHandler(_h)
                    _fallback.setLevel(logging.DEBUG)
                _orig = getattr(_fallback, attribute)

                # Wrap to strip extra kwargs (e.g. category=) that stdlib doesn't support
                def _safe_log(*args, _orig_fn=_orig, **kwargs):
                    # Forward Cara's ``category`` kwarg as ``extra`` so
                    # stdlib formatters can still access it if desired.
                    safe_kwargs = {
                        k: v
                        for k, v in kwargs.items()
                        if k in ("exc_info", "stack_info", "stacklevel", "extra")
                    }
                    category = kwargs.get("category")
                    if category:
                        extra = safe_kwargs.get("extra", {})
                        extra["category"] = category
                        safe_kwargs["extra"] = extra
                    return _orig_fn(*args, **safe_kwargs)

                return _safe_log
            if cls.key == "DB":
                # Fallback: use DatabaseManager directly when bootstrap fails
                from cara.eloquent.DatabaseManager import DatabaseManager

                _db = DatabaseManager.get_instance()
                return getattr(_db, attribute)
            if cls.key == "validation":
                # Fallback: use Validation directly when bootstrap fails (common in scripts)
                from cara.validation import Validation

                return getattr(Validation, attribute)
            # No fallback available - raise clear error
            raise CaraException(
                f"Facade '{cls.key}' is unavailable: application container not bootstrapped. "
                f"Ensure the application is initialized (SupportProvider registers it)."
            )

        # Handle IPython introspection methods
        if cls._is_private_method(attribute):
            raise AttributeError(
                f"'{cls.__name__}' object has no attribute '{attribute}'"
            )

        try:
            service = application.make(cls.key)
            return getattr(service, attribute)
        except Exception as e:
            logger = cls.get_logger()
            logger.error("Facade resolution failed for '%s': %s", cls.key, str(e))
            raise AttributeError(
                f"Facade '{cls.key}' could not resolve '{attribute}': {str(e)}"
            ) from e

    @classmethod
    def _is_private_method(cls, attribute: str) -> bool:
        """Check if attribute is a private/introspection method.

        Args:
            attribute: The attribute name to check

        Returns:
            True if the attribute is a private or introspection method
        """
        private_methods = {
            "_ipython_canary_method_should_not_exist_",
            "_ipython_display_",
            "_repr_mimebundle_",
            "_repr_html_",
            "_repr_json_",
            "_repr_latex_",
            "_repr_javascript_",
            "_repr_png_",
            "_repr_jpeg_",
            "_repr_svg_",
            # asyncio.iscoroutinefunction() / inspect probe these
            # single-underscore sentinels on whatever object they're handed.
            "_is_coroutine_marker",
            "_is_coroutine",
        }
        return (
            attribute.startswith("_ipython_")
            or attribute.startswith("_repr_")
            or attribute in private_methods
            # Any dunder (``__partialmethod__``, ``__func__``, ``__wrapped__``,
            # ``__test__``, ``__deepcopy__`` …) only reaches ``__getattr__``
            # because it is genuinely absent on the facade class — real
            # dunders resolve through normal type lookup and never get here.
            # The correct response is a quiet AttributeError, not an ERROR
            # log + container round-trip. Before this guard, framework
            # introspection (asyncio / functools / inspect / pytest) spammed
            # 90+ "Facade resolution failed for 'DB'" ERROR lines per run.
            or (attribute.startswith("__") and attribute.endswith("__"))
        )

    def __repr__(cls) -> str:
        """Provide a clean representation for IPython."""
        return f"<Facade: {cls.key}>"

    def __str__(cls) -> str:
        """Provide a clean string representation."""
        return f"Facade({cls.key})"

    @classmethod
    def get_logger(cls) -> Logger:
        """Get a logger instance for this facade.

        Returns:
            A logger instance configured for this facade
        """
        try:
            from cara.logging import Logger

            return Logger(name=cls.key)
        except ImportError:
            # Fallback if logging is not available
            import logging

            return logging.getLogger(cls.key)
