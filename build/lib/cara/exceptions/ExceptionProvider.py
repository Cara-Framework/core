"""
ExceptionProvider:

Binds a default exception handler and ensures that any exception
escaping the HTTP conductor is forwarded to that handler.
"""

from cara.foundation import Provider
from cara.exceptions.handlers import DefaultExceptionHandler


class ExceptionProvider(Provider):
    def __init__(self, application):
        self.application = application

    def register(self) -> None:
        # Bind DefaultExceptionHandler under "exception.handler"
        self.application.bind(
            "exception.handler",
            lambda: DefaultExceptionHandler(self.application),
        )

    def boot(self) -> None:
        # Monkey-patch HttpConductor.handle_request
        from cara.conductors.http import HttpConductor

        original_handle_request = HttpConductor.handle_request

        async def wrapped_handle_request(self, scope, receive, send):
            """
            1. Invoke the original handle_request (including middleware chain).
            2. If any exception bubbles up, retrieve "exception.handler" from container and call handle().
            """
            try:
                await original_handle_request(self, scope, receive, send)
            except Exception as exc:
                handler = self.application.make("exception.handler")
                await handler.handle(exc, self.request, scope, receive, send)
                return

        HttpConductor.handle_request = wrapped_handle_request
