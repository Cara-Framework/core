"""HTTP Request Provider."""

from __future__ import annotations

from cara.foundation import DeferredProvider
from cara.http.request.Request import Request
from cara.support import Image


class RequestProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["request", "image"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register HTTP Request and Image processing."""

        self.application.bind("request", lambda: Request(self.application))
        self.application.bind("image", Image())
