"""UniqueJob contract — prevents duplicate job dispatches.

Jobs implementing this contract are deduplicated by ``unique_id()`` in the
transactional queue-delivery ledger. PostgreSQL owns both the delivery and its
uniqueness fence, so duplicates return the existing pollable delivery UUID.

Usage:
    class RefreshProductJob(ShouldQueue, Queueable, UniqueJob):
        def __init__(self, product_id):
            self.product_id = product_id

        def unique_id(self) -> str:
            return f"refresh_product_{self.product_id}"

"""

from __future__ import annotations


class UniqueJob:
    """Mixin for jobs that should only run one instance at a time."""

    def unique_id(self) -> str:
        """Return a unique identifier for this job instance.
        Must be overridden by subclasses.
        """
        raise NotImplementedError("UniqueJob must implement unique_id()")
