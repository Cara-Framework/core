"""Plain-data rules for objects a queued framework job rebuilds on the worker.

A queued mailable, notification or notifiable crosses the broker as a fully
qualified class path plus JSON data, and is rebuilt on the worker through
the class's own ``from_queue_payload``. The signed envelope carries JSON
primitives only, by design, so anything else is refused HERE, at dispatch,
with the path of the offending field — not as an opaque serializer error,
and never by pickling or stringifying the object.
"""

from __future__ import annotations

import importlib
import math
from typing import Any

from cara.exceptions import InvalidArgumentException, QueueException

from .JobClassResolver import JobClassResolver


class QueuePayload:
    """Validate queue payload data and resolve the classes it names."""

    MAX_DEPTH = 32

    @classmethod
    def require_plain(cls, value: Any, field: str, *, _depth: int = 0) -> Any:
        """``value`` as JSON-able plain data (tuples become lists), or raise.

        The message names the field (``mailable_data.view_data.user``) so the
        caller sees which attribute to replace with a durable id.
        """
        if _depth > cls.MAX_DEPTH:
            raise InvalidArgumentException(f"{field} is nested too deeply to queue.")
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise InvalidArgumentException(
                    f"{field} is a non-finite float and cannot be queued."
                )
            return value
        if isinstance(value, (list, tuple)):
            return [
                cls.require_plain(item, f"{field}[{index}]", _depth=_depth + 1)
                for index, item in enumerate(value)
            ]
        if isinstance(value, dict):
            plain: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise InvalidArgumentException(
                        f"{field} has a non-string key {key!r} and cannot be queued."
                    )
                plain[key] = cls.require_plain(item, f"{field}.{key}", _depth=_depth + 1)
            return plain
        raise InvalidArgumentException(
            f"{field} is a {type(value).__name__}, which a queued job cannot carry: "
            "queue payloads are plain JSON data. Store a durable id (and load it "
            "on the worker) or override to_queue_payload()/from_queue_payload()."
        )

    @staticmethod
    def class_path(cls_: type) -> str:
        """The importable dotted path of ``cls_``, or raise.

        A class defined inside a function has ``<locals>`` in its qualified
        name and no worker can import it, so it is refused at dispatch.
        """
        qualname = cls_.__qualname__
        if "<locals>" in qualname:
            raise InvalidArgumentException(
                f"{cls_.__module__}.{qualname} is defined inside a function; "
                "a queued object must be an importable module-level class."
            )
        return f"{cls_.__module__}.{qualname}"

    #: The framework packages that define the queued-object contracts. A
    #: product's own classes are trusted through the queued-job allowlist
    #: (``JobClassResolver.allowed_prefixes``); these are added on top, and
    #: nothing else of ``cara`` is.
    FRAMEWORK_ROOTS = ("cara.mail", "cara.notifications")

    @classmethod
    def allowed_roots(cls) -> tuple[str, ...]:
        """Module roots a queued class path may name: the queued-job
        allowlist plus :data:`FRAMEWORK_ROOTS`."""
        return tuple(
            dict.fromkeys((*JobClassResolver.allowed_prefixes(), *cls.FRAMEWORK_ROOTS))
        )

    @classmethod
    def require_trusted(cls, path: str) -> str:
        """``path`` if it lies under :meth:`allowed_roots`, or raise.

        Checked on the TEXT, before anything is imported: an import runs the
        module's top level, so a subclass check after it comes too late.
        Called at dispatch too, so a class the worker would refuse fails at
        the call site rather than as a dead-lettered job.
        """
        if isinstance(path, str) and any(
            path.startswith(f"{root}.") for root in cls.allowed_roots()
        ):
            return path
        raise QueueException(
            f"Queued class {path!r} is outside the queued-class allowlist "
            "(queue.job_allowed_prefixes plus the framework's mail and "
            "notification packages)."
        )

    @classmethod
    def resolve_class(cls, path: str, base: type) -> type:
        """Import the class ``path`` names and require it to subclass ``base``.

        The path comes from an authenticated envelope, but the HMAC was the
        only thing between a forged payload and importing ANY module the
        path named, before the subclass check ever ran. The path must now lie
        under :meth:`allowed_roots` before anything is imported, and the
        class must subclass ``base`` — a real contract, never ``object``.
        """
        parts = path.split(".") if isinstance(path, str) else []
        if len(parts) < 2 or not all(
            part.isidentifier() and not part.startswith("__") for part in parts
        ):
            raise QueueException(f"Invalid queued class path: {path!r}")
        if base is object:
            raise QueueException(
                "A queued class must be resolved against its contract, not object."
            )
        cls.require_trusted(path)
        roots = cls.allowed_roots()
        # The split between module and qualified name is not marked, so the
        # longest importable module prefix wins (a nested class keeps its
        # outer class in the attribute walk).
        for split in range(len(parts) - 1, 0, -1):
            module_name = ".".join(parts[:split])
            if not any(
                module_name == root or module_name.startswith(f"{root}.")
                for root in roots
            ):
                # A shorter prefix would leave the trusted root (``app`` of
                # ``app.jobs``): the class is not importable from inside it.
                break
            try:
                target: Any = importlib.import_module(module_name)
            except ModuleNotFoundError as exc:
                if exc.name is not None and not module_name.startswith(exc.name):
                    # The module exists but one of ITS imports is missing:
                    # that is a broken module, not a shorter path to try.
                    raise QueueException(
                        f"Cannot import queued class {path}: {exc}"
                    ) from exc
                continue
            try:
                for attribute in parts[split:]:
                    target = getattr(target, attribute)
            except AttributeError as exc:
                raise QueueException(
                    f"Cannot resolve queued class {path}: {exc}"
                ) from exc
            if not isinstance(target, type) or not issubclass(target, base):
                raise QueueException(
                    f"Queued class {path} must be a subclass of {base.__name__}."
                )
            return target
        raise QueueException(f"Cannot import queued class {path}: no such module.")


__all__ = ["QueuePayload"]
