"""
Facade pattern implementation for Cara framework.

Provides a clean way to access services from the container as static-like methods.
Similar to Laravel facades - services are resolved on first access.
"""

from typing import Any, Optional


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
            AttributeError: If service cannot be resolved or attribute doesn't exist
        """
        try:
            from bootstrap import application
        except ImportError as e:
            raise RuntimeError(
                f"Cannot initialize facade '{cls.__name__}': bootstrap not available"
            ) from e

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
            logger.error(f"Facade resolution failed for '{cls.key}': {str(e)}")
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
        }
        return attribute.startswith("_ipython_") or attribute.startswith(
            "_repr_"
        ) or attribute in private_methods

    def __repr__(cls) -> str:
        """Provide a clean representation for IPython."""
        return f"<Facade: {cls.key}>"

    def __str__(cls) -> str:
        """Provide a clean string representation."""
        return f"Facade({cls.key})"

    @classmethod
    def get_logger(cls) -> "Logger":
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
