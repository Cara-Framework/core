"""
Base Job class for Cara Framework.

This class provides DRY functionality specifically for job operations
that need to be queued for background processing.
"""

from typing import Any, Dict

from .BaseQueueable import BaseQueueable


class BaseJob(BaseQueueable):
    """
    Base class for queueable job operations.

    Provides common functionality for all job classes that need
    to be processed in background queues.
    """

    # Job-specific queue settings
    default_queue: str = "jobs"
    default_retry_attempts: int = 3

    def __init__(self, payload=None, **kwargs):
        """Initialize queueable job with payload."""
        self.payload = payload or {}
        super().__init__(**kwargs)

    def _initialize_data(self, **kwargs):
        """Job-specific initialization."""
        # Set job priority
        self.job_priority = kwargs.get("priority", "normal")

        # Set job timeout
        self.timeout = kwargs.get("timeout", 300)  # 5 minutes default

        # Set job tags for monitoring
        self.tags = kwargs.get("tags", [])

    def with_payload(self, payload: Dict[str, Any]) -> "BaseJob":
        """Set or update job payload."""
        if isinstance(payload, dict):
            self.payload.update(payload)
        else:
            self.payload = payload
        return self

    def with_tag(self, tag: str) -> "BaseJob":
        """Add a tag to this job."""
        if tag not in self.tags:
            self.tags.append(tag)
        return self

    def with_tags(self, tags: list) -> "BaseJob":
        """Add multiple tags to this job."""
        for tag in tags:
            self.with_tag(tag)
        return self

    def priority(self, level: str) -> "BaseJob":
        """Set job priority level."""
        self.job_priority = level

        # Automatically adjust queue based on priority
        if level == "high":
            self.queue_name = "jobs-priority"
        elif level == "low":
            self.queue_name = "jobs-low"

        return self

    def high_priority(self) -> "BaseJob":
        """Mark this job as high priority."""
        return self.priority("high")

    def low_priority(self) -> "BaseJob":
        """Mark this job as low priority."""
        return self.priority("low")

    def timeout_minutes(self, minutes: int) -> "BaseJob":
        """Set job timeout in minutes."""
        self.timeout = minutes * 60
        return self

    def timeout_hours(self, hours: int) -> "BaseJob":
        """Set job timeout in hours."""
        self.timeout = hours * 3600
        return self

    def display_name(self) -> str:
        """Generate display name for job queue monitoring."""
        class_name = self.__class__.__name__

        if isinstance(self.payload, dict):
            action = self.payload.get("action", "job")
            return f"{class_name}: {action}"

        return f"{class_name}: job"

    def get_queue_options(self) -> Dict[str, Any]:
        """Get job-specific queue options."""
        options = super().get_queue_options()

        # Add job-specific options
        options.update(
            {
                "priority": self.job_priority,
                "type": "job",
                "timeout": self.timeout,
                "tags": self.tags,
                "payload_size": len(str(self.payload)) if self.payload else 0,
            }
        )

        return options

    def handle(self) -> Any:
        """
        Handle job execution.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Job classes must implement handle() method")

    def progress(self, current: int, total: int, message: str = ""):
        """
        Report job progress.
        Useful for long-running jobs.
        """
        progress_data = {
            "current": current,
            "total": total,
            "percentage": round((current / total) * 100, 2) if total > 0 else 0,
            "message": message,
        }

        # Log progress
        try:
            from cara.facades import Log

            Log.info(
                f"Job {self.__class__.__name__} progress: {progress_data['percentage']}% - {message}"
            )
        except ImportError:
            # Silently fail if Log facade not available - this is a framework component
            pass

        return progress_data

    def get_payload_value(self, key: str, default=None):
        """Get value from payload safely."""
        if isinstance(self.payload, dict):
            return self.payload.get(key, default)
        return default

    def set_payload_value(self, key: str, value: Any):
        """Set value in payload safely."""
        if not isinstance(self.payload, dict):
            self.payload = {}
        self.payload[key] = value

    def failed(self, job_data: Any, error: Exception) -> None:
        """Handle job failure."""
        try:
            from cara.facades import Log

            Log.error(f"Job {self.__class__.__name__} failed: {str(error)}")
            Log.error(f"Job payload: {self.payload}")
            Log.error(f"Job tags: {self.tags}")
        except ImportError:
            # Silently fail if Log facade not available - this is a framework component
            pass

        # Call parent failed method
        super().failed(job_data, error)
