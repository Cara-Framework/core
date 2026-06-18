"""
Pickle Job Serializer - Traditional Python serialization.

Use JsonJobSerializer for new projects (more secure and portable).
"""

from __future__ import annotations

import io
import pickle
from typing import Any


class _RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that only allows job-related modules.

    Unrestricted ``pickle.loads()`` is an arbitrary-code-execution
    vector: a crafted payload can import any module and call any
    callable. This restricted variant only permits classes under
    ``app.jobs`` / ``app.commands`` (the application's own job
    hierarchy) and safe builtins needed to reconstruct the payload
    dict (``dict``, ``tuple``, ``list``, ``set``, ``frozenset``,
    ``bytes``, ``bytearray``).  Everything else raises
    ``pickle.UnpicklingError``.
    """

    _ALLOWED_JOB_PREFIXES = (
        "app.jobs",
        "app.commands",
        # Framework internals that get serialised as part of
        # chain / batch dispatch payloads.
        "cara.queues",
    )

    _ALLOWED_BUILTINS = frozenset({
        "dict", "list", "tuple", "set", "frozenset",
        "bytes", "bytearray", "True", "False", "None",
        "int", "float", "str", "bool", "complex",
    })

    def find_class(self, module: str, name: str) -> Any:
        if module == "builtins" and name in self._ALLOWED_BUILTINS:
            return super().find_class(module, name)
        if any(module.startswith(prefix) for prefix in self._ALLOWED_JOB_PREFIXES):
            return super().find_class(module, name)
        raise pickle.UnpicklingError(
            f"Restricted unpickler denied: {module}.{name}"
        )


def restricted_pickle_loads(data: bytes) -> Any:
    """Drop-in replacement for ``pickle.loads`` that restricts allowed classes.

    Queue drivers and cache layers that still need pickle compat should
    call this instead of bare ``pickle.loads``.
    """
    return _RestrictedUnpickler(io.BytesIO(data)).load()


class PickleJobSerializer:
    """
    Serialize jobs using Python's pickle module with restricted
    deserialization.

    Deserialization uses ``_RestrictedUnpickler`` which only permits
    job classes from ``app.jobs``, ``app.commands``, and
    ``cara.queues``. Arbitrary code execution via crafted payloads
    is blocked.

    For new projects, prefer JsonJobSerializer.
    """

    @staticmethod
    def serialize(
        job_class: type, init_args: tuple = (), init_kwargs: dict | None = None
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
    def deserialize(pickle_bytes: bytes) -> dict[str, Any]:
        """
        Deserialize pickle bytes to job specification using the
        restricted unpickler.

        Args:
            pickle_bytes: Pickle payload

        Returns:
            Dict with 'class', 'args', 'kwargs'
        """
        payload = restricted_pickle_loads(pickle_bytes)

        return {
            "class": payload.get("obj"),
            "args": payload.get("args", ()),
            "kwargs": payload.get("kwargs", {}),
        }
