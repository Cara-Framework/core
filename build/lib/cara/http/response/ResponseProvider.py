"""HTTP Response Provider."""

from cara.foundation import DeferredProvider


class ResponseProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["response"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register HTTP Response."""
        from cara.http import Response

        self.application.bind("response", lambda: Response(self.application))
