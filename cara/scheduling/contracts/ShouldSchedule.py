"""
ShouldSchedule Contract for the Cara framework.

This module defines the interface for jobs that should be scheduled, specifying the required method
for schedule eligibility.
"""

from abc import ABC, abstractmethod
from cara.scheduling import Scheduling


class ShouldSchedule(ABC):
    @classmethod
    @abstractmethod
    def schedule(cls, scheduling: Scheduling) -> None:
        """Register scheduling instructions on the given Scheduling instance."""
        pass
