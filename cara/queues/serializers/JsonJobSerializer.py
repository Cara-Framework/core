"""
JSON Job Serializer - Modern alternative to pickle.

Serializes jobs as JSON instead of pickle for:
- Security (no arbitrary code execution)
- Portability (cross-language compatible)
- Debugging (human-readable payloads)
- Simplicity (only primitives allowed)
"""

import json
from typing import Any, Dict


class JsonJobSerializer:
    """
    Serialize jobs to JSON format.

    Jobs must follow these rules:
    1. __init__ accepts only primitives (str, int, dict, list, etc.)
    2. No complex objects in constructor
    3. All dependencies are lazy-loaded or injected via context

    This enforces clean architecture and eliminates pickle issues.
    """

    @staticmethod
    def serialize(
        job_class: type, init_args: tuple = (), init_kwargs: dict = None
    ) -> str:
        """
        Serialize job to JSON string.

        Args:
            job_class: Job class (not instance)
            init_args: Positional arguments for __init__
            init_kwargs: Keyword arguments for __init__

        Returns:
            JSON string representing the job

        Example:
            >>> JsonJobSerializer.serialize(MyJob, (), {"product_id": 123})
            '{"class": "MyJob", "module": "app.jobs", "args": [], "kwargs": {"product_id": 123}}'
        """
        init_kwargs = init_kwargs or {}

        payload = {
            "class": job_class.__name__,
            "module": job_class.__module__,
            "args": list(init_args),
            "kwargs": init_kwargs,
        }

        try:
            return json.dumps(payload, default=JsonJobSerializer._json_default)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Job {job_class.__name__} has non-serializable parameters. "
                f"Only primitives (str, int, dict, list) are allowed in __init__. "
                f"Error: {e}"
            )

    @staticmethod
    def deserialize(json_string: str) -> Dict[str, Any]:
        """
        Deserialize JSON string to job specification.

        Args:
            json_string: JSON payload

        Returns:
            Dict with 'class', 'module', 'args', 'kwargs'

        Example:
            >>> spec = JsonJobSerializer.deserialize(json_str)
            >>> job_class = spec["class"]
            >>> job = job_class(*spec["args"], **spec["kwargs"])
        """
        try:
            payload = json.loads(json_string)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

        # Import the job class dynamically
        module_name = payload.get("module")
        class_name = payload.get("class")

        if not module_name or not class_name:
            raise ValueError("Missing 'module' or 'class' in payload")

        try:
            import importlib

            module = importlib.import_module(module_name)
            job_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Cannot import {module_name}.{class_name}: {e}")

        return {
            "class": job_class,
            "args": tuple(payload.get("args", [])),
            "kwargs": payload.get("kwargs", {}),
        }

    @staticmethod
    def _json_default(obj):
        """Custom JSON encoder for special types."""
        # Handle datetime objects
        if hasattr(obj, "isoformat"):
            return obj.isoformat()

        # Handle Pydantic models
        if hasattr(obj, "model_dump"):
            return obj.model_dump()

        # Handle dataclasses
        if hasattr(obj, "__dataclass_fields__"):
            from dataclasses import asdict

            return asdict(obj)

        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
