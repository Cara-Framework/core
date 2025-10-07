"""
Serializer Factory - Creates appropriate serializer based on config.

Generic factory pattern for pluggable serialization.
"""

from typing import Union

from .serializers import JsonJobSerializer, PickleJobSerializer


class SerializerFactory:
    """
    Factory for creating job serializers.

    Reads from config to determine which serializer to use.
    Completely generic - no app-specific logic.
    """

    _serializers = {
        "json": JsonJobSerializer,
        "pickle": PickleJobSerializer,
    }

    @classmethod
    def create(
        cls, serializer_type: str = None
    ) -> Union[JsonJobSerializer, PickleJobSerializer]:
        """
        Create serializer instance based on type.

        Args:
            serializer_type: 'json' or 'pickle' (reads from config if None)

        Returns:
            Serializer instance

        Raises:
            ValueError: If serializer type is unknown
        """
        if serializer_type is None:
            # Try app config first, fall back to framework config
            serializer_type = cls._get_config_serializer()

        serializer_class = cls._serializers.get(serializer_type.lower())

        if not serializer_class:
            raise ValueError(
                f"Unknown serializer type: {serializer_type}. "
                f"Available: {list(cls._serializers.keys())}"
            )

        return serializer_class()

    @classmethod
    def _get_config_serializer(cls) -> str:
        """Get serializer type from app config."""
        try:
            import config.queue as queue_config

            return queue_config.JOB_SERIALIZER
        except (ImportError, AttributeError):
            # Default to JSON (modern, secure, enforces clean architecture)
            return "json"
