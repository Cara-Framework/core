"""
Job Context - Generic Dependency Injection for Jobs.

Provides thread-safe dependency injection without any app-specific knowledge.
Pure infrastructure code - completely generic and reusable.
"""

from contextvars import ContextVar
from typing import Any, Dict, Optional


class JobContext:
    """
    Generic dependency injection container for jobs.

    100% framework-level code - no app-specific knowledge.
    Uses ContextVar for thread-safe dependency injection.

    App Usage:
        class MyJob:
            def handle(self):
                repo = JobContext.get('repository')
                config = JobContext.get('config')

    Framework Usage:
        with JobContext.provide(repository=repo, config=cfg):
            await job.handle()
    """

    _container: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
        "job_context_container", default=None
    )

    @classmethod
    def provide(cls, **dependencies):
        """
        Context manager to inject dependencies into job execution scope.

        Args:
            **dependencies: Any key-value pairs to inject (app decides keys)

        Returns:
            Context manager for with statement

        Example:
            with JobContext.provide(db=db_conn, cache=redis_conn):
                await job.handle()
        """
        token = cls._container.set(dependencies)

        class ContextManager:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                cls._container.reset(token)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                cls._container.reset(token)

        return ContextManager()

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        """
        Retrieve dependency from context by key.

        Args:
            key: Dependency identifier (app defines these)
            default: Fallback value if key not found

        Returns:
            Dependency value or default
        """
        container = cls._container.get()
        if container is None:
            return default
        return container.get(key, default)

    @classmethod
    def has(cls, key: str) -> bool:
        """Check if dependency exists in current context."""
        container = cls._container.get()
        return container is not None and key in container

    @classmethod
    def all(cls) -> Dict[str, Any]:
        """Get all dependencies (useful for debugging)."""
        return cls._container.get() or {}

    @classmethod
    def clear(cls):
        """Clear context (mainly for testing)."""
        cls._container.set(None)
