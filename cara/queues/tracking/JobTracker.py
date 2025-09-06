"""
Advanced Job Tracker for Cara Framework.

App-agnostic job tracking service with smart retry logic, conflict resolution,
and performance analytics. Similar to Laravel's job tracking but enhanced.
"""

import uuid
from typing import Any, Dict, List, Optional

import pendulum

from cara.facades import Log


class JobTracker:
    """
    Advanced job tracking and management service for Cara Framework.
    
    Features:
    - Smart retry with exponential backoff
    - Job chaining and dependencies  
    - Conflict resolution (prevent duplicate jobs)
    - Performance analytics
    - Dead letter queue management
    - App-agnostic design
    
    Usage:
        # In app, optionally provide JobLog model
        from cara.queues.tracking import JobTracker
        
        # Configure with app's JobLog model (optional)
        tracker = JobTracker(job_log_model=MyJobLogModel)
        
        # Or use without persistence (logs only)
        tracker = JobTracker()
    """

    # Default retry configuration (apps can override)
    DEFAULT_MAX_RETRIES = {
        'default': 3
    }
    
    DEFAULT_RETRY_DELAYS = [60, 300, 900]  # 1min, 5min, 15min
    
    def __init__(self, job_log_model=None, max_retries: Dict[str, int] = None, 
                 retry_delays: List[int] = None):
        """
        Initialize JobTracker with optional app-specific configuration.
        
        Args:
            job_log_model: Optional JobLog model class for persistence
            max_retries: Dict of job_name -> max_retry_count
            retry_delays: List of delay seconds for retries
        """
        self.job_log_model = job_log_model
        self.max_retries = max_retries or self.DEFAULT_MAX_RETRIES
        self.retry_delays = retry_delays or self.DEFAULT_RETRY_DELAYS
        
        # Try to auto-detect JobLog model if not provided
        if not self.job_log_model:
            self.job_log_model = self._try_get_job_log_model()
    
    def track_job_started(self, job_uid: str, job_name: str, job_id: int = None, entity_id: str = None, 
                         queue: str = 'default', metadata: Dict = None) -> str:
        """
        Track job start with conflict detection.
        
        Args:
            job_uid: Unique job UUID identifier for tracking
            job_name: Job class name
            job_id: Job.id (integer FK) from driver - required for job_logs FK
            entity_id: Optional entity ID (product_id, user_id, etc.)
            queue: Queue name
            metadata: Additional metadata
            
        Returns:
            str: The job_uid for tracking
        """
        try:
            # Cancel conflicting jobs for same entity
            if entity_id:
                self._cancel_conflicting_jobs(job_name, entity_id, job_uid)
            
            # Create job log entry if model available and job_id provided
            if self.job_log_model and job_id:
                self.job_log_model.create_job_log(
                    job_name=job_name,
                    job_id=job_id,  # Required FK to job.id
                    job_uid=job_uid,
                    queue=queue,
                    entity_id=entity_id,
                    metadata=metadata or {}
                )
            elif self.job_log_model and not job_id:
                Log.warning(f"JobTracker: No job_id provided for {job_name}, skipping job_logs entry")
            
            Log.info(f"🚀 Job started: {job_name}[{job_uid}] for entity {entity_id}", category="cara.queue.jobs")
            return job_uid
            
        except Exception as e:
            Log.warning(f"Failed to track job start: {str(e)}")
            return job_uid
    
    def track_job_processing(self, job_uid: str) -> None:
        """Mark job as actively processing."""
        try:
            if self.job_log_model:
                self.job_log_model.mark_processing(job_uid)
            Log.info(f"⚡ Job processing: {job_uid}", category="cara.queue.jobs")
        except Exception as e:
            Log.warning(f"Failed to mark job as processing: {str(e)}")
    
    def track_job_success(self, job_uid: str, result_data: Dict = None) -> None:
        """Track successful job completion."""
        try:
            # Update job log
            if self.job_log_model:
                self.job_log_model.mark_success(job_uid)
                
                # Store result metadata if provided
                if result_data:
                    job_log = self.job_log_model.where('job_uid', job_uid).first()
                    if job_log:
                        metadata = job_log.metadata or {}
                        metadata['result'] = result_data
                        job_log.update({'metadata': metadata})
            
            Log.info(f"✅ Job completed: {job_uid}", category="cara.queue.jobs")
        except Exception as e:
            Log.warning(f"Failed to track job success: {str(e)}")
    
    def track_job_failed(self, job_uid: str, error: str, should_retry: bool = True) -> Optional[str]:
        """
        Track job failure and handle retry logic.
        
        Args:
            job_uid: Current job UID
            error: Error message
            should_retry: Whether to attempt retry
            
        Returns:
            Optional[str]: New job_uid if retry scheduled, None if max retries exceeded
        """
        try:
            if not self.job_log_model:
                Log.error(f"💥 Job failed: {job_uid} - {error}", category="cara.queue.jobs")
                return None
                
            # Get current job info
            job_log = self.job_log_model.where('job_uid', job_uid).first()
            if not job_log:
                Log.error(f"Job log not found for {job_uid}", category="cara.queue.jobs")
                return None
            
            # Mark current attempt as failed
            self.job_log_model.mark_failed(job_uid, error)
            
            # Check if we should retry
            max_retries = self.max_retries.get(job_log.job_name, self.max_retries['default'])
            
            if should_retry and job_log.attempt < max_retries:
                return self._schedule_retry(job_log, error)
            else:
                self._move_to_dead_letter(job_log, error)
                Log.error(f"💀 Job failed permanently: {job_uid} after {job_log.attempt} attempts", category="cara.queue.jobs")
                return None
                
        except Exception as e:
            Log.warning(f"Failed to track job failure: {str(e)}")
            return None
    
    def should_job_continue(self, job_uid: str, entity_id: str = None) -> bool:
        """
        Check if job should continue processing based on job_uid.
        
        Args:
            job_uid: Current job UID (UUID string)
            entity_id: Optional entity ID for conflict checking
            
        Returns:
            bool: True if job should continue
        """
        try:
            if not self.job_log_model:
                return True
                
            job_log = self.job_log_model.where('job_uid', job_uid).first()
            if not job_log:
                return True
                
            # Check if job was cancelled or failed
            if job_log.status in [self.job_log_model.STATUS_CANCELLED]:
                return False
                
            return True
            
        except Exception as e:
            Log.warning(f"Error checking job status {job_uid}: {str(e)}")
            return True
    
    def validate_job_or_cancel(self, job_uid: str, entity_id: str = None, operation: str = "operation") -> None:
        """
        Validate job should continue or raise JobCancelledException.
        
        Args:
            job_uid: Current job UID (UUID string)
            entity_id: Optional entity ID
            operation: Operation name for logging
            
        Raises:
            JobCancelledException: If job should not continue
        """
        if not self.should_job_continue(job_uid, entity_id):
            # Lazy import to avoid circular dependency
            from cara.queues.contracts.CancellableJob import JobCancelledException
            raise JobCancelledException(f"Job {job_uid} cancelled during {operation} for entity {entity_id}")
    
    def get_job_analytics(self, entity_id: str = None, job_name: str = None, 
                         hours: int = 24) -> Dict[str, Any]:
        """
        Get job performance analytics.
        
        Args:
            entity_id: Optional entity filter
            job_name: Optional job name filter
            hours: Time window in hours
            
        Returns:
            Dict with analytics data
        """
        if not self.job_log_model:
            return {'total_jobs': 0, 'message': 'No JobLog model configured'}
            
        try:
            query = self.job_log_model.query()
            
            if entity_id:
                query = query.where('product_id', entity_id)
            if job_name:
                query = query.where('job_name', job_name)
                
            # Time window
            since = pendulum.now().subtract(hours=hours)
            jobs = query.where('created_at', '>=', since).get()
            
            total_jobs = len(jobs)
            if total_jobs == 0:
                return {'total_jobs': 0}
            
            # Status counts
            status_counts = {}
            for job in jobs:
                status = getattr(job, 'status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            # Average processing time for successful jobs
            successful_jobs = [j for j in jobs if getattr(j, 'status', None) == 'success' 
                             and hasattr(j, 'finished_at') and hasattr(j, 'processed_at')
                             and j.finished_at and j.processed_at]
            
            avg_processing_time = 0
            if successful_jobs:
                total_time = sum([
                    (j.finished_at - j.processed_at).total_seconds() 
                    for j in successful_jobs
                ])
                avg_processing_time = total_time / len(successful_jobs)
            
            success_count = status_counts.get('success', 0)
            
            return {
                'total_jobs': total_jobs,
                'status_counts': status_counts,
                'success_count': success_count,
                'success_rate': (success_count / total_jobs * 100) if total_jobs > 0 else 0,
                'avg_processing_time_seconds': avg_processing_time,
                'period_hours': hours
            }
            
        except Exception as e:
            Log.error(f"Failed to get job analytics: {str(e)}")
            return {'error': str(e)}
    
    def _cancel_conflicting_jobs(self, job_name: str, entity_id: str, current_job_uid: str) -> int:
        """
        Cancel conflicting jobs for same entity.
        
        Args:
            job_name: Job class name
            entity_id: Entity ID to check conflicts for
            current_job_uid: Current job UID to exclude
            
        Returns:
            int: Number of jobs cancelled
        """
        try:
            if not self.job_log_model:
                return 0
                
            # Find pending/processing jobs for same entity and job type
            conflicting_jobs = self.job_log_model.where('job_name', job_name)\
                                                 .where('entity_id', entity_id)\
                                                 .where('job_uid', '!=', current_job_uid)\
                                                 .where_in('status', ['pending', 'processing'])\
                                                 .get()
            
            cancelled_count = 0
            for job_log in conflicting_jobs:
                job_log.update({'status': self.job_log_model.STATUS_CANCELLED})
                cancelled_count += 1
                Log.info(f"Cancelled conflicting job: {job_log.job_uid} for entity {entity_id}")
            
            return cancelled_count
            
        except Exception as e:
            Log.warning(f"Failed to cancel conflicting jobs: {str(e)}")
            return 0
    
    def _schedule_retry(self, job_log, error: str) -> str:
        """Schedule job retry with exponential backoff."""
        try:
            next_attempt = job_log.attempt + 1
            delay_seconds = self.retry_delays[min(next_attempt - 1, len(self.retry_delays) - 1)]
            
            # Create new job log for retry
            retry_job_uid = str(uuid.uuid4())
            
            metadata = getattr(job_log, 'metadata', {}) or {}
            metadata['retry_reason'] = error
            metadata['original_job_uid'] = job_log.job_uid
            metadata['scheduled_for'] = pendulum.now().add(seconds=delay_seconds).to_iso8601_string()
            
            self.job_log_model.create({
                'job_name': job_log.job_name,
                'job_id': job_log.job_id,  # Use same job_id (FK to job table)
                'job_uid': retry_job_uid,  # New UUID for retry tracking
                'entity_id': getattr(job_log, 'entity_id', None),
                'status': 'retrying',
                'attempt': next_attempt,
                'queue': getattr(job_log, 'queue', 'default'),
                'metadata': metadata
            })
            
            Log.info(f"🔄 Retry scheduled: {job_log.job_name}[{retry_job_uid}] attempt {next_attempt} in {delay_seconds}s")
            return retry_job_uid
            
        except Exception as e:
            Log.error(f"Failed to schedule retry: {str(e)}")
            return None
    
    def _move_to_dead_letter(self, job_log, final_error: str) -> None:
        """Move permanently failed job to dead letter queue."""
        try:
            if hasattr(job_log, 'metadata'):
                metadata = getattr(job_log, 'metadata', {}) or {}
                metadata['dead_letter_reason'] = final_error
                metadata['moved_to_dlq_at'] = pendulum.now().to_iso8601_string()
                job_log.update({'metadata': metadata})
            
            Log.error(f"💀 Job moved to dead letter: {job_log.job_name}[{job_log.job_uid}] - {final_error}")
            
        except Exception as e:
            Log.warning(f"Failed to move job to dead letter: {str(e)}")
    
    def _try_get_job_log_model(self):
        """Try to auto-detect JobLog model from app."""
        try:
            # Try common app patterns
            from app.models import JobLog
            return JobLog
        except ImportError:
            pass
            
        try:
            from commons.models.core import JobLog
            return JobLog
        except ImportError:
            pass
            
        # No model found, tracking will be logs-only
        return None 