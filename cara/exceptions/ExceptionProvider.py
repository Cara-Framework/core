"""
ExceptionProvider:

Binds a default exception handler and ensures that any exception
escaping the HTTP conductor is forwarded to that handler.
"""

from __future__ import annotations

import importlib

# Direct submodule import — NOT ``from cara.foundation import Provider``.
# ``cara.exceptions`` is pulled in DURING ``cara.foundation`` boot (via
# ``cara.environment``), so the foundation package is mid-init and its
# namespace hasn't bound the ``Provider`` CLASS yet — the package import would
# resolve to the SUBMODULE and ``class ExceptionProvider(Provider)`` would
# raise "module() takes at most 2 arguments". Importing the submodule directly
# loads ``Provider`` (which only depends on ``abc``) regardless of the
# foundation package's init state.
from cara.foundation import Provider


def _default_handler(application):
    """Resolve HTTP response machinery only when the binding is consumed.

    Importing the exception taxonomy is foundational and must not eagerly boot
    middleware, configuration, support utilities and conductors. The handler
    is an application-edge collaborator, so its module belongs at container
    resolution time after those packages have completed initialization.
    """
    module = importlib.import_module("cara.exceptions.handlers.DefaultExceptionHandler")
    return module.DefaultExceptionHandler(application)


class ExceptionProvider(Provider):
    def __init__(self, application):
        self.application = application

    def register(self) -> None:
        # Bind DefaultExceptionHandler under "exception.handler"
        self.application.bind(
            "exception.handler",
            lambda: _default_handler(self.application),
        )

    def boot(self) -> None:
        # Monkey-patch HttpConductor._handle_request
        from cara.conductors.http import (
            HttpConductor,  # local: cycle with cara.conductors.http.HttpConductor
        )

        original_handle_request = HttpConductor._handle_request

        async def wrapped_handle_request(self, scope, receive, send, request, response):
            """
            1. Invoke the original _handle_request (including middleware chain).
            2. If any exception bubbles up, retrieve "exception.handler" from
               container and call handle().

            Note: request and response are per-request local variables passed
            from handle() — NOT stored on self (concurrency-safe).
            """
            try:
                await original_handle_request(
                    self, scope, receive, send, request, response
                )
            except Exception as exc:
                handler = self.application.make("exception.handler")
                await handler.handle(exc, request, scope, receive, send)
                return

        HttpConductor._handle_request = wrapped_handle_request
