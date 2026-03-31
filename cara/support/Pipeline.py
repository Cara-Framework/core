"""Pipeline system for sequential data processing.

Provides a flexible pipeline implementation for processing data through a series of
handlers. Each handler can modify the input before passing it to the next stage.

Similar to Laravel's Pipeline implementation - enables middleware chains and other
sequential processing patterns.
"""

from typing import Any, Awaitable, Callable, List, Optional, Type, Union


class Pipeline:
    """Pipeline class for sequential data processing.

    Implements a pipeline pattern where data can be processed through a series of pipes
    (middleware/handlers). Each pipe can transform the data and control the flow to the
    next pipe in the sequence.

    Supports both synchronous and asynchronous pipes. Each pipe should have a handle()
    method that accepts the request and a next callable.
    """

    def __init__(
        self, passable: Any, application: Optional[Any] = None
    ) -> None:
        """Initialize the pipeline.

        Args:
            passable: The object to pass through the pipeline
            application: Optional application instance for dependency injection
        """
        self.passable = passable
        self.application = application
        self.pipes: List[Union[Type, Any]] = []

    def through(self, pipes: List[Union[Type, Any]]) -> "Pipeline":
        """Set the objects to send through the pipeline.

        Args:
            pipes: List of pipe objects or pipe classes to process

        Returns:
            Self for method chaining
        """
        self.pipes = pipes
        return self

    async def __call__(
        self, final_handler: Optional[Callable[[Any], Awaitable[Any]]] = None
    ) -> Any:
        """Execute the pipeline.

        Args:
            final_handler: Optional final handler to call at the end

        Returns:
            The processed passable after going through all pipes
        """

        async def call_pipe(index: int, request: Any) -> Any:
            # If we've processed all pipes, call the final handler or return
            if index >= len(self.pipes):
                if final_handler:
                    return await final_handler(request)
                return request

            # Get the current pipe
            pipe = self.pipes[index]

            # Instantiate pipe if it's a class
            if isinstance(pipe, type):
                pipe = pipe(self.application) if self.application else pipe()

            # Call the pipe's handle method with a closure for the next pipe
            result = await pipe.handle(
                request,
                lambda r: call_pipe(index + 1, r),
            )
            return result

        result = await call_pipe(0, self.passable)
        return result
