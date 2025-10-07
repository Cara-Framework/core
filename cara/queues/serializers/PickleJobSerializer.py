"""
Pickle Job Serializer - Traditional Python serialization.

Legacy serializer for backward compatibility.
Use JsonJobSerializer for new projects (more secure and portable).
"""

import pickle
from typing import Any, Dict


class PickleJobSerializer:
    """
    Serialize jobs using Python's pickle module.

    ⚠️ Security Warning: Pickle can execute arbitrary code during deserialization.
    Only use with trusted job sources.

    For new projects, prefer JsonJobSerializer.
    """

    @staticmethod
    def serialize(
        job_class: type, init_args: tuple = (), init_kwargs: dict = None
    ) -> bytes:
        """
        Serialize job to pickle bytes.

        Args:
            job_class: Job class (not instance)
            init_args: Positional arguments for __init__
            init_kwargs: Keyword arguments for __init__

        Returns:
            Pickle bytes representing the job
        """
        init_kwargs = init_kwargs or {}

        payload = {
            "obj": job_class,
            "args": init_args,
            "kwargs": init_kwargs,
            "callback": "handle",
        }

        return pickle.dumps(payload)

    @staticmethod
    def deserialize(pickle_bytes: bytes) -> Dict[str, Any]:
        """
        Deserialize pickle bytes to job specification.

        Args:
            pickle_bytes: Pickle payload

        Returns:
            Dict with 'class', 'args', 'kwargs'
        """
        payload = pickle.loads(pickle_bytes)

        return {
            "class": payload.get("obj"),
            "args": payload.get("args", ()),
            "kwargs": payload.get("kwargs", {}),
        }
