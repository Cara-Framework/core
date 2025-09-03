"""
Base provider definitions for service registration and bootstrapping.

This module defines DeferredProvider (deferred until first
requested), which all concrete providers should extend.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import List

from cara.foundation import Provider


class DeferredProvider(Provider):
    """
    A DeferredProvider is not registered at bootstrap; instead, its .register() and .boot() methods
    only run the first time one of its declared keys is resolved.

    Subclasses **must** implement:

        @classmethod
        def provides(cls) -> List[str]:
            return ["key1", "key2", …]

    The Application container uses this list of keys to defer the actual binding
    until someone calls app.make("key1") or app.make("key2").
    """

    @classmethod
    @abstractmethod
    def provides(cls) -> List[str]:
        """
        Return the list of string keys that this provider will bind into the container.

        Example:
            class FooProvider(DeferredProvider):
                @classmethod
                def provides(cls) -> List[str]:
                    return ["foo", "foo_manager"]

                def register(self):
                    self.application.bind("foo", Foo())
                    self.application.bind("foo_manager", FooManager(self.application))

                def boot(self):
                    pass
        """
