"""HTTP Request Provider."""

from cara.foundation import DeferredProvider


class RequestProvider(DeferredProvider):
    @classmethod
    def provides(cls):
        return ["request", "image"]

    def __init__(self, application):
        self.application = application

    def register(self):
        """Register HTTP Request and Image processing."""
        from cara.http import Request
        from cara.support.Image import Image

        self.application.bind("request", lambda: Request(self.application))
        self.application.bind("image", Image())
