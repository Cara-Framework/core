"""
Loader Provider for the Cara framework.

This module provides the service provider that binds the Loader utility into the application
container.
"""

from cara.foundation import DeferredProvider
from cara.loader import Loader


class LoaderProvider(DeferredProvider):
    """
    LoaderProvider:
    Binds 'loader' when first requested.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["loader"]

    def __init__(self, application):
        self.application = application

    def register(self) -> None:
        # At first app.make("loader"), bind the Loader instance
        self.application.bind("loader", Loader())

    def boot(self) -> None:
        # No additional boot logic needed
        pass
