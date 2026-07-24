"""Allowlisted queued-job class resolution after envelope authentication."""

from __future__ import annotations

import importlib
from collections.abc import Iterable

from cara.exceptions import QueueException
from cara.queues.contracts import ShouldQueue


class JobClassResolver:
    """Resolve only configured ``ShouldQueue`` classes."""

    _DEFAULT_JOB_PREFIXES = (
        "app.jobs",
        "app.commands",
        "cara.queues",
    )

    @classmethod
    def allowed_prefixes(
        cls,
        explicit: Iterable[str] | str | None = None,
    ) -> tuple[str, ...]:
        """Return normalized dynamic-import trust roots for queued jobs."""
        if explicit is not None:
            prefixes = explicit
        else:
            try:
                from cara.configuration import config

                prefixes = (
                    config("queue.drivers.amqp.allowed_job_prefixes", None)
                    or config("queue.job_allowed_prefixes", None)
                    or cls._DEFAULT_JOB_PREFIXES
                )
            except Exception:
                prefixes = cls._DEFAULT_JOB_PREFIXES

        if isinstance(prefixes, str):
            prefixes = (prefixes,)
        if not isinstance(prefixes, Iterable):
            raise QueueException("Queued-job module allowlist must be iterable.")

        normalized = tuple(
            str(prefix).strip().rstrip(".") for prefix in prefixes if str(prefix).strip()
        )
        if not normalized:
            raise QueueException("Queued-job module allowlist must not be empty.")
        if any(
            not all(part.isidentifier() for part in prefix.split("."))
            for prefix in normalized
        ):
            raise QueueException(
                "Queued-job module allowlist contains an invalid prefix."
            )
        return normalized

    @classmethod
    def resolve(
        cls,
        module_name: str,
        class_name: str,
        *,
        allowed_prefixes: Iterable[str] | str | None = None,
    ) -> type[ShouldQueue]:
        """Resolve one segment-aware, allowlisted ``ShouldQueue`` class."""
        if not isinstance(module_name, str) or not isinstance(class_name, str):
            raise QueueException("Queued job module and class must be strings.")
        if not all(part.isidentifier() for part in module_name.split(".")):
            raise QueueException(f"Invalid queued-job module: {module_name!r}")
        if not class_name.isidentifier() or class_name.startswith("_"):
            raise QueueException(f"Invalid queued-job class: {class_name!r}")

        prefixes = cls.allowed_prefixes(allowed_prefixes)
        if not any(
            module_name == prefix or module_name.startswith(f"{prefix}.")
            for prefix in prefixes
        ):
            raise QueueException(
                f"Queued job module {module_name!r} is outside the configured allowlist."
            )

        try:
            module = importlib.import_module(module_name)
            job_class = getattr(module, class_name)
        except (ImportError, AttributeError) as exc:
            raise QueueException(
                f"Cannot import queued job {module_name}.{class_name}: {exc}"
            ) from exc

        if not isinstance(job_class, type) or not issubclass(job_class, ShouldQueue):
            raise QueueException(
                f"Queued job {module_name}.{class_name} must implement ShouldQueue."
            )
        return job_class
