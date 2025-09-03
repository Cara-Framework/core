"""
Helper Functions Provider for the Cara framework.

This module provides a service provider that registers global helper functions and utilities. It
makes common framework functionality available globally through Python's builtin namespace.
"""

import builtins

from cara.foundation import Provider


class SupportProvider(Provider):
    def __init__(self, application):
        self.application = application

    def register(self):
        def app(service_name=None):
            """Get application instance or resolve service."""
            if service_name:
                return self.application.make(service_name)
            return self.application

        builtins.app = app

    def boot(self):
        pass
