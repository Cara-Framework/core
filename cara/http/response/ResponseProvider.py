"""HTTP Response Provider."""

from __future__ import annotations

from cara.foundation import DeferredProvider
from cara.http.response.Response import Response


class ResponseProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["response"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register HTTP Response."""

        self.application.bind("response", lambda: Response(self.application))
