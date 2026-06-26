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
    hierarchy), a closed set of safe immutable value types that jobs
    routinely carry in their ``args`` / ``kwargs`` (``datetime``,
    ``pendulum`` temporals, ``Decimal``, ``UUID``), and safe builtins
    needed to reconstruct the payload dict (``dict``, ``tuple``,
    ``list``, ``set``, ``frozenset``, ``bytes``, ``bytearray``).
    Everything else raises ``pickle.UnpicklingError``.
    """

    # Framework-safe default trust roots. App-specific prefixes — the app's
    # own job packages and any monorepo layout (e.g. ``packages.<name>``
    # or a shared ``commons.jobs`` tree) — are supplied by the APP via
    # ``config/queue.py::pickle_allowed_prefixes`` so the framework's unpickle
    # trust boundary encodes no app filesystem layout. See ``_allowed_prefixes``.
    _DEFAULT_JOB_PREFIXES = (
        "app.jobs",  # Cara-app job convention
        "app.commands",
        "cara.queues",  # framework chain/batch dispatch envelopes
    )

    _ALLOWED_BUILTINS = frozenset({
        "dict", "list", "tuple", "set", "frozenset",
        "bytes", "bytearray", "True", "False", "None",
        "int", "float", "str", "bool", "complex",
    })

    # Safe, immutable value types that legitimately appear inside a job's
    # serialized ``args`` / ``kwargs`` (e.g. a ``pendulum.now()`` timestamp
    # passed to a scheduled job, a ``Decimal`` price, a ``UUID`` correlation
    # id). Before this allowlist, ANY job whose payload carried one of these
    # — pendulum DateTimes especially, since the pipeline uses pendulum
    # everywhere — failed to deserialize on dequeue and was dead-lettered
    # with "Restricted unpickler denied: pendulum.datetime.DateTime". These
    # are pure data classes: constructing them runs no side effects, so
    # allowlisting them by EXACT (module, name) keeps the anti-RCE posture
    # intact while making the serializer round-trip the values jobs actually
    # use. Matched by exact pair (not prefix) so e.g. ``decimal.<other>`` or
    # an attacker-supplied ``pendulum.<gadget>`` stays denied.
    _ALLOWED_VALUE_CLASSES = frozenset({
        ("datetime", "datetime"),
        ("datetime", "date"),
        ("datetime", "time"),
        ("datetime", "timedelta"),
        ("datetime", "timezone"),
        ("decimal", "Decimal"),
        ("uuid", "UUID"),
        ("pendulum.datetime", "DateTime"),
        ("pendulum.date", "Date"),
        ("pendulum.time", "Time"),
        ("pendulum.duration", "Duration"),
        ("pendulum.tz.timezone", "Timezone"),
        ("pendulum.tz.timezone", "FixedTimezone"),
    })

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Resolve the trust-root prefixes once per payload (config is loaded
        # by deserialize time). Framework defaults + app-configured extras.
        self._job_prefixes = self._allowed_prefixes()

    @classmethod
    def _allowed_prefixes(cls) -> tuple[str, ...]:
        """Framework defaults plus app-configured extra trust-root prefixes.

        The app declares its own job trust roots (its package layout,
        monorepo-shared job trees) in
        ``config/queue.py::pickle_allowed_prefixes``; the framework holds
        only generic defaults, so its RCE trust boundary encodes no app
        filesystem layout.
        """
        try:
            from cara.configuration import config

            extra = tuple(
                str(p) for p in (config("queue.pickle_allowed_prefixes", ()) or ())
            )
        except Exception:
            extra = ()
        return cls._DEFAULT_JOB_PREFIXES + extra

    @staticmethod
    def _guarded_getattr(obj: Any, name: Any) -> Any:
        """``getattr`` exposed to the unpickler with dunder access blocked.

        ``pendulum``'s ``Timezone`` reduce reconstructs itself via
        ``getattr(Timezone, "_unpickle")`` + REDUCE, so the unpickler must
        resolve ``builtins.getattr``. Exposing the bare builtin would,
        however, hand a crafted payload the canonical pickle escalation
        primitive: ``getattr(<allowed cls>, "__init__").__globals__`` →
        ``getattr(dict, "__getitem__")`` → ``os`` → RCE. Blocking every
        dunder name severs that chain (``__globals__``, ``__getitem__``,
        ``__class__``, … all denied) while still permitting the public
        ``_unpickle`` hook pendulum relies on. Non-dunder attributes of the
        allowlisted *value* classes are plain methods/classmethods with no
        reachable side-effecting gadget.
        """
        if not isinstance(name, str) or name.startswith("__"):
            raise pickle.UnpicklingError(
                f"Restricted unpickler denied getattr of {name!r}"
            )
        return getattr(obj, name)

    def find_class(self, module: str, name: str) -> Any:
        if module == "builtins" and name in self._ALLOWED_BUILTINS:
            return super().find_class(module, name)
        if module == "builtins" and name == "getattr":
            # Needed by pendulum temporal reduces; hardened against the
            # getattr-gadget escalation by ``_guarded_getattr``.
            return self._guarded_getattr
        if (module, name) in self._ALLOWED_VALUE_CLASSES:
            return super().find_class(module, name)
        if any(module.startswith(prefix) for prefix in self._job_prefixes):
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
