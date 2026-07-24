"""best_effort — decorator and context manager for non-critical side effects.

Centralizes the try/log/continue pattern so call sites become one-liners
instead of repeating try/except blocks. Works for both sync and async
functions.

Usage::

    @best_effort(category="images")
    def sync_side_effect(data): ...


    @best_effort(category="prices")
    async def async_side_effect(data): ...


    with best_effort_ctx("notifications"):
        send_notification(...)
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

_best_effort_failure_counts: dict[str, int] = {}


def _log_facade():
    """Resolve the ``Log`` facade lazily.

    ``cara.support`` is a lower layer than ``cara.facades``; importing the ``Log``
    facade at module top creates a support→facades→support cycle that aborts when
    ``cara.facades.Facade`` is imported first (e.g. by the test harness's
    ``facade_swap``). Resolving at call time keeps the layering acyclic.
    """
    from cara.facades import Log

    return Log


def best_effort(
    category: str = "best_effort",
    *,
    default: Any = None,
    log_level: str = "warning",
) -> Callable:
    """Decorator: swallow exceptions and log at the given level.

    Returns ``default`` on failure. Never raises (except
    ``KeyboardInterrupt`` / ``SystemExit``).

    Tracks per-function failure counts in ``_best_effort_failure_counts``
    so monitoring can surface chronically failing helpers.
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            except KeyboardInterrupt, SystemExit:
                raise
            except Exception as e:
                key = f"{category}.{fn.__qualname__}"
                _best_effort_failure_counts[key] = (
                    _best_effort_failure_counts.get(key, 0) + 1
                )
                getattr(_log_facade(), log_level)(
                    f"[best_effort] {fn.__qualname__} failed (count={_best_effort_failure_counts[key]}): {e}",
                    category=category,
                    exc_info=True,
                )
                return default

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except KeyboardInterrupt, SystemExit:
                raise
            except Exception as e:
                key = f"{category}.{fn.__qualname__}"
                _best_effort_failure_counts[key] = (
                    _best_effort_failure_counts.get(key, 0) + 1
                )
                getattr(_log_facade(), log_level)(
                    f"[best_effort] {fn.__qualname__} failed (count={_best_effort_failure_counts[key]}): {e}",
                    category=category,
                    exc_info=True,
                )
                return default

        if asyncio.iscoroutinefunction(fn):
            return async_wrapper
        return wrapper

    return decorator


@contextmanager
def best_effort_ctx(
    category: str = "best_effort",
    *,
    label: str | None = None,
    log_level: str = "warning",
):
    """Context manager version for inline blocks.

    Usage::

        with best_effort_ctx("price_snapshot"):
            record_price(...)
    """
    try:
        yield
    except KeyboardInterrupt, SystemExit:
        raise
    except Exception as e:
        msg = f"[best_effort] {label or 'block'} failed: {e}"
        getattr(_log_facade(), log_level)(msg, category=category)


__all__ = ["best_effort", "best_effort_ctx"]
