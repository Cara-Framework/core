"""
Trackable Trait for Laravel-style job lifecycle tracking.

This trait provides automatic job tracking capabilities for any job class.
Similar to Laravel's job tracking but integrated into the Cara framework.
"""

import uuid
from typing import Any, Dict, Optional

from cara.facades import Log


class Trackable:
    """
    Trackable trait for automatic job lifecycle tracking.
    
    Laravel-style trait to add tracking capabilities to any job class.
    Integrates with Cara's queue system to provide comprehensive job monitoring.
    
    Usage:
        from cara.queues.tracking import Trackable
        
        class MyJob(Trackable, Queueable, ShouldQueue):
            def __init__(self, entity_id):
                super().__init__()
                self.entity_id = entity_id
                
            async def handle(self):
                # Tracking is automatic!
                await self.do_work()
    
    Features:
        - Automatic job lifecycle tracking
        - Conflict resolution (prevent duplicate jobs)
        - Smart retry with exponential backoff
        - Performance analytics
        - Laravel-style method chaining
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize tracking properties."""
        super().__init__(*args, **kwargs)
        self._job_uid: Optional[str] = None
        self._tracking_metadata: Dict[str, Any] = {}
        self._tracking_enabled: bool = True
        self._job_tracker: Optional[Any] = None
        
        # Ensure priority attribute exists for queue system
        if not hasattr(self, 'priority'):
            self.priority = "default"
        
    def with_tracking(self, enabled: bool = True) -> "Trackable":
        """Enable/disable job tracking (Laravel-style fluent method)."""
        self._tracking_enabled = enabled
        return self
        
    def with_metadata(self, metadata: Dict[str, Any]) -> "Trackable":
        """Set metadata for job tracking (Laravel-style fluent method)."""
        self._tracking_metadata.update(metadata)
        return self
    
    def set_tracking_metadata(self, key: str, value: Any) -> None:
        """Set individual metadata key for job tracking."""
        self._tracking_metadata[key] = value
    
    def get_job_id(self) -> Optional[str]:
        """Get the current job tracking ID."""
        return self._job_uid
    
    def is_tracking_enabled(self) -> bool:
        """Check if tracking is enabled for this job."""
        return self._tracking_enabled
    
    def _start_tracking(self) -> Optional[str]:
        """Start job tracking and return job_id."""
        if not self._tracking_enabled:
            return None
            
        try:
            # Generate unique job ID
            self._job_uid = str(uuid.uuid4())
            
            # Get job information
            job_name = self.__class__.__name__
            entity_id = self._get_entity_id()
            queue = self.queue if hasattr(self, 'queue') else 'default'
            
            # Get database job ID if available (from AMQP driver)
            db_job_id = getattr(self, '_db_job_id', None)
            
            # Get JobTracker instance
            job_tracker = self._get_job_tracker()
            if job_tracker:
                job_tracker.track_job_started(
                    job_uid=self._job_uid,
                    job_name=job_name,
                    job_id=db_job_id,  # Pass database job ID for FK
                    entity_id=entity_id,
                    queue=queue,
                    metadata=self._tracking_metadata
                )
            
            Log.info(f"🚀 Job tracking started: {job_name}[{self._job_uid}] for entity {entity_id}", category="cara.queue.jobs")
            return self._job_uid
            
        except Exception as e:
            Log.warning(f"Failed to start job tracking: {str(e)}")
            return None
    
    def _mark_processing(self) -> None:
        """Mark job as processing."""
        if not self._tracking_enabled or not self._job_uid:
            return
            
        try:
            job_tracker = self._get_job_tracker()
            if job_tracker:
                job_tracker.track_job_processing(self._job_uid)
            Log.info(f"⚡ Job processing: {self._job_uid}", category="cara.queue.jobs")
        except Exception as e:
            Log.warning(f"Failed to mark job as processing: {str(e)}")
    
    def _mark_success(self, result_data: Dict = None) -> None:
        """Mark job as successful."""
        if not self._tracking_enabled or not self._job_uid:
            return
            
        try:
            job_tracker = self._get_job_tracker()
            if job_tracker:
                job_tracker.track_job_success(self._job_uid, result_data)
            Log.info(f"✅ Job completed: {self._job_uid}", category="cara.queue.jobs")
        except Exception as e:
            Log.warning(f"Failed to mark job as successful: {str(e)}")
    
    def _mark_failed(self, error: str, should_retry: bool = True) -> Optional[str]:
        """Mark job as failed and handle retry logic."""
        if not self._tracking_enabled or not self._job_uid:
            return None
            
        try:
            job_tracker = self._get_job_tracker()
            if job_tracker:
                return job_tracker.track_job_failed(self._job_uid, error, should_retry)
            Log.error(f"💥 Job failed: {self._job_uid} - {error}", category="cara.queue.jobs")
        except Exception as e:
            Log.warning(f"Failed to mark job as failed: {str(e)}")
        return None
    
    def _should_continue(self) -> bool:
        """Check if job should continue processing."""
        if not self._tracking_enabled or not self._job_uid:
            return True
            
        try:
            job_tracker = self._get_job_tracker()
            if job_tracker:
                entity_id = self._get_entity_id()
                return job_tracker.should_job_continue(self._job_uid, entity_id)
        except Exception as e:
            Log.warning(f"Failed to check job continuation: {str(e)}")
        return True
    
    def _validate_or_cancel(self, operation: str = "operation") -> None:
        """Validate job should continue or raise JobCancelledException."""
        if not self._tracking_enabled or not self._job_uid:
            return
            
        try:
            job_tracker = self._get_job_tracker()
            if job_tracker:
                entity_id = self._get_entity_id()
                job_tracker.validate_job_or_cancel(self._job_uid, entity_id, operation)
        except Exception as e:
            # Re-raise specific exceptions but log others
            if e.__class__.__name__ == "JobCancelledException":
                raise
            Log.warning(f"Failed to validate job continuation: {str(e)}")
    
    def _get_entity_id(self) -> Optional[str]:
        """
        Get entity ID for this job (app-specific).
        
        Override this method or provide common attribute names.
        """
        # Common attribute names for entity identification
        for attr in ['entity_id', 'product_id', 'amazon_product_id', 'user_id', 'receipt_id', 'id']:
            if hasattr(self, attr):
                value = getattr(self, attr)
                return str(value) if value is not None else None
        return None
    
    def _get_job_tracker(self):
        """
        Get job tracker instance (cached).
        
        Uses Cara's JobTracker with optional app-specific JobLog model.
        """
        if self._job_tracker is not None:
            return self._job_tracker
            
        try:
            # Import JobTracker from same package
            from .JobTracker import JobTracker

            # Try to get app-specific JobLog model
            job_log_model = None
            try:
                from commons.models.core import JobLog
                job_log_model = JobLog
            except ImportError:
                try:
                    from app.models import JobLog
                    job_log_model = JobLog
                except ImportError:
                    pass
            
            # Create JobTracker instance
            self._job_tracker = JobTracker(job_log_model=job_log_model)
            return self._job_tracker
            
        except ImportError:
            Log.warning("JobTracker not available, tracking disabled")
            self._job_tracker = None
            return None
    
    def display_name(self) -> str:
        """
        Enhanced display name for queue monitoring with tracking info.
        
        Override the base display_name to include tracking information.
        """
        base_name = super().display_name() if hasattr(super(), 'display_name') else self.__class__.__name__
        
        if self._job_uid:
            return f"{base_name} [{self._job_uid[:8]}]"
        return base_name 