"""
Pipeline system for sequential data processing.

This module provides a flexible pipeline implementation for processing data through a series of
handlers. It enables sequential processing where each handler can modify the input before passing it
to the next stage.
"""

from typing import Any, List


class Pipeline:
    """
    Pipeline class for sequential data processing.

    This class implements a pipeline pattern where data can be processed through a series of pipes
    (handlers). Each pipe can transform the data and control the flow to the next pipe in the
    sequence.
    """

    def __init__(self, passable: Any, application=None):
        """
        Initialize the pipeline.

        Args:
            passable: The object to pass through the pipeline
            application: Optional application instance
        """
        self.passable = passable
        self.application = application

    def through(self, pipes: List[Any]):
        """
        Set the objects to send through the pipeline.

        Args:
            pipes: List of pipe objects to process

        Returns:
            Callable: A function that executes the pipeline
        """
        self.pipes = pipes
        return self

    async def __call__(self, final_handler=None) -> Any:
        async def call_pipe(index: int, request: Any) -> Any:
            if index >= len(self.pipes):
                if final_handler:
                    result = await final_handler(request)
                    return result
                return request
            else:
                pipe = self.pipes[index]
                if isinstance(pipe, type):
                    pipe = pipe(self.application) if self.application else pipe()

                result = await pipe.handle(
                    request,
                    lambda r: call_pipe(index + 1, r),
                )
                return result

        result = await call_pipe(0, self.passable)
        return result
