"""
Python Logging Adapter for Cara Framework.

This adapter makes Cara Logger compatible with Python's standard logging interface,
allowing external libraries to use Cara's logging system seamlessly.
Laravel-style approach: inject our logger into external libraries with category support.
"""

from __future__ import annotations

import logging

from cara.facades import Log

from .CaraLoggerFactory import CaraLoggerFactory
from .CaraPythonLoggerAdapter import CaraPythonLoggerAdapter


def install_cara_loggers():
    """
    Install Cara loggers for all external libraries.
    Laravel-style: inject our logger into external libraries.
    """
    # Laravel-style library configuration with appropriate defaults
    library_configs = {
        # Web Server (usually noisy, so DEBUG by default)
        "uvicorn.error": "INFO",  # Server errors - keep visible
        "uvicorn.access": "DEBUG",  # Access logs - usually too noisy
        "uvicorn": "DEBUG",  # General uvicorn logs
        # HTTP Clients (can be useful for debugging API calls)
        "httpx": "INFO",  # HTTP requests - good for debugging
        "httpcore": "DEBUG",  # Low-level HTTP - usually too detailed
        "urllib3": "DEBUG",  # urllib3 details - noisy
        "requests": "INFO",  # HTTP requests - useful
        # Database ORM (very noisy)
        "eloquent.models.hydrate": "DEBUG",  # Model hydration - very noisy
        "eloquent.models": "DEBUG",  # General model logs
        "eloquent": "DEBUG",  # ORM logs
        # Message Queues (important for debugging)
        "pika": "INFO",  # RabbitMQ - keep for queue debugging
        # Cache & Storage
        "redis": "DEBUG",  # Redis operations - can be noisy
        "aioredis": "DEBUG",  # Async Redis
        # Scheduler & Background Tasks
        "apscheduler": "WARNING",  # Scheduler - only important events
        # WebSockets (connection events)
        "websockets": "INFO",  # WebSocket connections - useful
    }

    for library_name, level in library_configs.items():
        # Create Cara-compatible logger
        cara_logger = CaraLoggerFactory.create_for_library(library_name, level)

        # Replace the library's logger
        original_logger = logging.getLogger(library_name)
        original_logger.handlers.clear()
        original_logger.propagate = False
        original_logger.setLevel(getattr(logging, level))

        # Monkey patch to use our adapter
        logging.getLogger(library_name).__class__ = CaraPythonLoggerAdapter
        logging.getLogger(library_name).cara_logger = Log
        logging.getLogger(library_name).library_name = library_name
        logging.getLogger(library_name).category = cara_logger.category
