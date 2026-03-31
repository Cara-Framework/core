"""
Interface marker for queueable classes.

This module defines a marker interface that indicates a class should be processed through the queue
system rather than synchronously.
"""


class ShouldQueue:
    pass
