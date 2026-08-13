"""A different open delivery already owns the unique job key."""

from cara.exceptions import QueueException


class UniqueDeliveryConflict(QueueException):
    """A different open delivery already owns the unique job key."""

    def __init__(self, job_id: str):
        self.job_id = str(job_id)
        super().__init__(f"Unique job already has open delivery {self.job_id}.")
