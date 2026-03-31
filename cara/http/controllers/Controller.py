"""
HTTP Controller Module for REST APIs.

This module provides the base Controller class for the Cara framework, implementing Laravel-style
controller functionality with dependency injection, middleware support, and standardized response
handling.
"""

from cara.http import Request, Response


class Controller:
    def __init__(self):
        """Initialize the controller with request and response properties."""
        self.application = app()
        self.request: Request = None  # Populated when the controller is invoked
        self.response: Response = None  # Populated when the controller is invoked

    def __call__(
        self,
        request: Request,
        response: Response,
        *args,
        **kwargs,
    ):
        """
        WSGI compatibility method.
        This method makes the controller callable, allowing it to be used directly
        as a request handler in the routing system. It sets up the request and response
        objects and delegates to the handle method.
        """
        self.request = request
        self.response = response
        return self.handle(request, response, *args, **kwargs)

    def handle(
        self,
        request: Request,
        response: Response,
        *args,
        **kwargs,
    ):
        """
        Handle the HTTP request and generate a response.
        This method should be overridden by child controller classes to implement
        specific request handling logic. The base implementation raises NotImplementedError
        to ensure child classes provide their own implementation.
        """
        raise NotImplementedError("Controller must implement handle() method.")
