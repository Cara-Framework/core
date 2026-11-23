"""
Python Logging Adapter for Cara Framework.

Routes every stdlib ``logging`` record of the process through the ONE Cara
pipeline (redaction, the ``Log`` facade's channels, ``CategoryFilter``):

* the framework's own ``cara.*`` loggers;
* the products' loggers (``app.*``, ``shared.*``, ``commons.*`` and any name
  in ``logging.python_levels``), at their own configured level;
* every other (third-party) logger from WARNING up by default, or at the
  level the library table / ``logging.python_levels`` gives it.

A library logger is no longer cut off from the hierarchy: before, the table's
loggers were swapped to an adapter class and set ``propagate = False``, so
their CHILD loggers (``pika.channel``, ``apscheduler.scheduler``,
``urllib3.connectionpool``) found no handler and fell through to
``logging.lastResort`` — raw, unredacted lines on stderr — while the
products' loggers reached the root ``NullHandler`` and were discarded.
"""

from __future__ import annotations

import logging
import sys
import threading

from cara.facades import Log
from cara.support import redact_log_secrets

from .CaraPythonLoggerAdapter import CaraPythonLoggerAdapter

#: The framework's own stdlib logger namespace (``logging.getLogger("cara.…")``).
_FRAMEWORK_LOGGER = "cara"
#: The boot-time fallback of the ``Log`` facade; it prints for itself, and
#: bridging it would feed the facade's failure back into the facade.
_FALLBACK_LOGGER = "cara.fallback"
#: Top-level logger names of product code. Their records pass at the level the
#: product set on them (DEBUG when unset: the Cara channel level and the
#: category rules decide), never capped at the third-party WARNING default.
_PRODUCT_NAMESPACES = ("app", "shared", "commons")
#: Level of every logger nobody configured: third-party chatter stays out.
_THIRD_PARTY_DEFAULT_LEVEL = logging.WARNING
#: Laravel-style per-library defaults; ``logging.python_levels`` overrides them.
_LIBRARY_LEVELS = {
    # Web Server (usually noisy, so DEBUG by default)
    "uvicorn.error": "INFO",  # Server errors - keep visible
    "uvicorn.access": "DEBUG",  # Access logs - usually too noisy
    "uvicorn": "DEBUG",  # General uvicorn logs
    # HTTP Clients (can be useful for debugging API calls)
    "httpx": "INFO",
    "httpcore": "DEBUG",
    "urllib3": "DEBUG",
    "requests": "INFO",
    # Database ORM (very noisy)
    "eloquent.models.hydrate": "DEBUG",
    "eloquent.models": "DEBUG",
    "eloquent": "DEBUG",
    # Message Queues (important for debugging)
    "pika": "INFO",
    # Cache & Storage
    "redis": "DEBUG",
    "aioredis": "DEBUG",
    # Scheduler & Background Tasks
    "apscheduler": "WARNING",
    # WebSockets (connection events)
    "websockets": "INFO",
}
_CARA_LEVELS = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "critical",
}
#: Marks a record already handed to Cara, so the ``cara`` bridge and the root
#: bridge (which both see a ``cara.*`` record) deliver it once.
_DELIVERED = "_cara_bridged"
_bridging = threading.local()


def _is_framework(name: str) -> bool:
    return name == _FRAMEWORK_LOGGER or name.startswith(_FRAMEWORK_LOGGER + ".")


def _is_product(name: str) -> bool:
    root = name.split(".", 1)[0]
    return root in _PRODUCT_NAMESPACES


def _route(record: logging.LogRecord) -> tuple[str, str, str]:
    """``(cara level, category, module column)`` of one stdlib record."""
    name = record.name
    if _is_framework(name) or _is_product(name) or name == "root":
        level = _CARA_LEVELS.get(record.levelno) or (
            "error" if record.levelno >= logging.ERROR else "warning"
        )
        return level, name, record.module
    category = CaraPythonLoggerAdapter.category_for(name)
    return (
        CaraPythonLoggerAdapter.cara_level_for(record.levelno, category),
        category,
        CaraPythonLoggerAdapter.display_name_for(name),
    )


def _report_undeliverable(
    record: logging.LogRecord, message: str, error: Exception
) -> None:
    """Last-resort line for a record the Cara logger could not take.

    Written straight to stderr (the stdlib's own ``Handler.handleError``
    convention) with the REDACTED message: going back through ``logging``
    would re-enter this bridge, and ``handleError`` would print the raw args.
    """
    stream = sys.stderr
    if stream is None:
        return
    try:
        stream.write(
            f"[cara.logging] undeliverable {record.levelname} {record.name}: "
            f"{message} ({type(error).__name__})\n"
        )
    except OSError, ValueError:
        # A closed or broken stderr leaves nowhere to report to.
        return


def _forward_record(record: logging.LogRecord) -> None:
    """Hand one stdlib record to the Cara logger.

    Never raises and never re-enters: the Cara logger failing (or logging
    through the stdlib itself) must not turn one warning into a recursion.
    """
    if (
        record.name.startswith(_FALLBACK_LOGGER)
        or getattr(record, _DELIVERED, False)
        or getattr(_bridging, "active", False)
    ):
        return
    setattr(record, _DELIVERED, True)
    _bridging.active = True
    message = ""
    try:
        level, category, module = _route(record)
        message = record.getMessage()
        prefix = f"[{record.name}]"
        if message.startswith(prefix):
            message = message[len(prefix) :].strip()
        message = redact_log_secrets(message)
        getattr(Log, level)(
            message,
            category=category,
            exc_info=record.exc_info,
            _module_override=module,
        )
    except Exception as error:
        _report_undeliverable(record, redact_log_secrets(message), error)
    finally:
        _bridging.active = False


# Kept under its historical name: it is the ``cara`` logger's handler body.
_forward_framework_record = _forward_record


def _bridge_handler() -> logging.Handler:
    bridge = logging.Handler()
    bridge.emit = _forward_record  # type: ignore[method-assign]
    bridge._cara_bridge = True  # type: ignore[attr-defined]
    return bridge


def _install_once(logger: logging.Logger) -> None:
    if not any(getattr(h, "_cara_bridge", False) for h in logger.handlers):
        logger.addHandler(_bridge_handler())


def _bridge_framework_loggers() -> None:
    """Route the framework's own stdlib loggers into the Cara logger.

    Installed on ``cara`` itself (not only on the root) so a framework warning
    survives a later reset of the root handlers. Idempotent.
    """
    _install_once(logging.getLogger(_FRAMEWORK_LOGGER))


def _bridge_root_logger() -> None:
    """Route every other stdlib logger (products, third parties) into Cara.

    Replaces the root ``NullHandler`` (and any stream handler a library or
    ``basicConfig`` left there, which would print the raw record a second
    time). Idempotent.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        if not getattr(handler, "_cara_bridge", False):
            root.removeHandler(handler)
    _install_once(root)
    root.setLevel(_THIRD_PARTY_DEFAULT_LEVEL)


def install_cara_loggers(levels: dict[str, str] | None = None):
    """Route all stdlib logging of the process through the Cara pipeline.

    ``levels`` (``logging.python_levels``, passed by ``LoggerProvider``) sets
    or overrides the level of named stdlib loggers over the library table.
    """
    configured: dict[str, str] = dict(_LIBRARY_LEVELS)
    if isinstance(levels, dict):
        configured.update({str(k): str(v) for k, v in levels.items()})
    for name, level in configured.items():
        library_logger = logging.getLogger(name)
        # Its own stream handlers would print the raw record beside Cara's.
        library_logger.handlers.clear()
        # Propagation is what carries the CHILD loggers' records to the root
        # bridge; cutting it sent them to ``logging.lastResort``.
        library_logger.propagate = True
        library_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    for namespace in _PRODUCT_NAMESPACES:
        product_logger = logging.getLogger(namespace)
        if product_logger.level == logging.NOTSET:
            product_logger.setLevel(logging.DEBUG)

    _bridge_framework_loggers()
    _bridge_root_logger()
