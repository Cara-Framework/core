"""
Base class for queueable tasks in the Cara framework.

This module provides the foundation for creating background tasks with retry capabilities and
failure handling. Includes automatic serialization support and job cancellation.
"""

from typing import Optional

from cara.queues.JobStateManager import get_job_state_manager
from cara.queues.tracking import JobTracker

from .CancellableJob import CancellableJob, JobCancelledException
from .SerializesModels import SerializesModels


class PendingDispatch:
    """
    Laravel-style PendingDispatch for method chaining.
    
    Allows chaining like: MyJob.dispatch().onQueue('high').delay(30)
    Enhanced with routing key support for topic exchange.
    """
    
    def __init__(self, job_instance):
        """Initialize with job instance."""
        self.job = job_instance
        self._queue_name = getattr(job_instance, 'queue', 'default')
        self._delay = None
        self._connection = None
        self._routing_key = None
        self._use_exchange = False
        
    def onQueue(self, queue: str) -> "PendingDispatch":
        """Set the queue name (Laravel naming convention)."""
        self._queue_name = queue
        if hasattr(self.job, 'queue'):
            self.job.queue = queue
        return self
        
    def on_queue(self, queue: str) -> "PendingDispatch":
        """Python naming alias for onQueue."""
        return self.onQueue(queue)
        
    def delay(self, seconds: int) -> "PendingDispatch":
        """Set delay in seconds."""
        self._delay = seconds
        return self
        
    def onConnection(self, connection: str) -> "PendingDispatch":
        """Set connection (Laravel naming)."""
        self._connection = connection
        return self
    
    def withRoutingKey(self, routing_key: str) -> "PendingDispatch":
        """
        Set routing key for topic exchange dispatch.
        
        Args:
            routing_key: Routing key (e.g., "enrichment.product.high")
            
        Usage:
            MyJob.dispatch().withRoutingKey("enrichment.product.high")
        """
        self._routing_key = routing_key
        self._use_exchange = True
        return self
    
    def toExchange(self, exchange_name: str = "cheapa.events") -> "PendingDispatch":
        """
        Force dispatch to specific exchange.
        
        Args:
            exchange_name: Name of the exchange
        """
        self._exchange_name = exchange_name
        self._use_exchange = True
        return self
        
    def __del__(self):
        """Auto-dispatch when PendingDispatch is garbage collected (Laravel pattern)."""
        self._dispatch_now()
        
    def _dispatch_now(self):
        """Actually dispatch the job to the queue."""
        try:
            # Check if we should use exchange routing
            if self._use_exchange and self._routing_key:
                return self._dispatch_via_exchange()
            
            # Standard queue dispatch
            from cara.facades import Queue

            # Set final queue properties
            if hasattr(self.job, 'queue'):
                self.job.queue = self._queue_name
                
            if self._delay and hasattr(self.job, 'delay'):
                self.job.delay = self._delay
                
            # Push to queue
            job_id = Queue.push(self.job)
            
            # Set tracking ID
            if hasattr(self.job, 'set_tracking_id'):
                self.job.set_tracking_id(str(job_id))
                
            return job_id
            
        except Exception as e:
            # Fallback to sync execution
            try:
                from cara.facades import Log
                Log.warning(f"Queue dispatch failed, running sync: {str(e)}")
            except:
                pass
                
            if hasattr(self.job, 'handle'):
                return self.job.handle()
    
    def _dispatch_via_exchange(self):
        """Dispatch job via topic exchange with routing key."""
        try:
            from cara.queues.exchanges import TopicExchange

            # Get or create exchange
            exchange_name = getattr(self, '_exchange_name', 'cheapa.events')
            exchange = TopicExchange(exchange_name)
            
            # Set job properties
            if self._delay and hasattr(self.job, 'delay'):
                self.job.delay = self._delay
            
            # Dispatch via exchange
            job_id = exchange.dispatch_job(
                routing_key=self._routing_key,
                job_instance=self.job
            )
            
            # Set tracking ID
            if hasattr(self.job, 'set_tracking_id'):
                self.job.set_tracking_id(str(job_id))
                
            return job_id
            
        except Exception as e:
            # Fallback to standard dispatch
            from cara.facades import Log
            Log.warning(f"Exchange dispatch failed, using standard queue: {str(e)}", category="cara.queue.exchange")
            
            # Remove exchange flags and retry standard dispatch
            self._use_exchange = False
            return self._dispatch_now()


class Queueable(SerializesModels, CancellableJob):
    """
    Makes classes Queueable with Laravel-style dispatch.

    The Queueable class is responsible for handling background tasks.
    Includes automatic serialization, cancellation support, and universal job tracking.
    """

    run_again_on_fail = True
    run_times = 3

    def __init__(self, *args, **kwargs):
        """Initialize queueable job."""
        super().__init__()  # CancellableJob.__init__() handles its own initialization
        self.job_tracking_id: Optional[str] = None
        self._job_state_manager = get_job_state_manager()
        self._job_tracker = JobTracker()
        self._db_record_id: Optional[str] = None  # Database tracking record ID
        
        # Laravel-style properties
        self.queue = "default"
        self.delay = None
        self.connection = None

    def set_tracking_id(self, tracking_id: str) -> "Queueable":
        """
        Set job tracking ID for cancellation management.

        Args:
            tracking_id: Unique identifier for job tracking

        Returns:
            self: For method chaining
        """
        self.job_tracking_id = tracking_id
        return self

    def should_continue(self) -> bool:
        """
        Check if job should continue execution.

        Override this method to implement custom cancellation logic.
        Default implementation checks job state manager.

        Returns:
            bool: True if job should continue, False if cancelled
        """
        if not self.job_tracking_id:
            return True

        return not self._job_state_manager.is_job_cancelled(self.job_tracking_id)

    def check_cancellation(self, operation: str = "operation") -> None:
        """
        Check if job has been cancelled and raise exception if so.

        Args:
            operation: Name of the operation being checked (for logging)

        Raises:
            JobCancelledException: If the job has been cancelled
        """
        if not self.should_continue():
            raise JobCancelledException(
                f"Job {self.job_tracking_id} was cancelled during {operation}"
            )

    def register_job(self, context: dict) -> None:
        """
        Register job with cancellation system.

        Args:
            context: Context dictionary containing job information
        """
        if self.job_tracking_id:
            self._job_state_manager.register_job(self.job_tracking_id, context)

    def mark_completed(self) -> None:
        """Mark job as completed in the tracking system."""
        if self.job_tracking_id:
            self._job_state_manager.mark_completed(self.job_tracking_id)

    def mark_failed(self, error: str) -> None:
        """
        Mark job as failed in the tracking system.

        Args:
            error: Error message describing the failure
        """
        if self.job_tracking_id:
            self._job_state_manager.mark_failed(self.job_tracking_id, error)

    def get_cancellation_context(self) -> dict:
        """
        Get context for job cancellation tracking.

        Override this method to provide specific cancellation context.

        Returns:
            dict: Context information for cancellation tracking
        """
        return {
            "job_class": self.__class__.__name__,
            "job_id": self.job_tracking_id,
        }

    def serialize(self) -> dict:
        """Serialize the job for storage."""
        return {
            **super().serialize(),
            "job_tracking_id": self.job_tracking_id,
            "queue": self.queue,
            "delay": self.delay,
            "connection": self.connection,
        }

    def unserialize(self, data: dict) -> None:
        """Unserialize the job from storage."""
        super().unserialize(data)
        self.job_tracking_id = data.get("job_tracking_id")
        self.queue = data.get("queue", "default")
        self.delay = data.get("delay")
        self.connection = data.get("connection")

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

    @classmethod
    def dispatch(cls, *args, **kwargs) -> PendingDispatch:
        """
        Laravel-style job dispatch with method chaining support.
        
        Returns PendingDispatch for chaining methods like onQueue(), delay(), etc.
        
        Usage:
            MyJob.dispatch(param1, param2).onQueue('high-priority').delay(30)
        """
        # Create job instance
        instance = cls(*args, **kwargs)
        
        # Return PendingDispatch for method chaining
        return PendingDispatch(instance)

    @classmethod
    def dispatchAfter(cls, delay, *args, **kwargs):
        """Laravel-style delayed job dispatch."""
        return cls.dispatch(*args, **kwargs).delay(delay)

    @classmethod
    async def dispatchNow(cls, *args, **kwargs):
        """Laravel-style immediate job execution."""
        instance = cls(*args, **kwargs)
        if hasattr(instance, 'handle'):
            if hasattr(instance.handle, '__call__'):
                # Check if handle is async
                import asyncio
                if asyncio.iscoroutinefunction(instance.handle):
                    return await instance.handle()
                else:
                    return instance.handle()
        return None

    def _safe_serialize(self) -> dict:
        """Safely serialize job data for database storage."""
        try:
            return self.serialize()
        except Exception:
            # Fallback to basic info if serialize fails
            return {
                "job_class": self.__class__.__name__,
                "job_id": getattr(self, "job_tracking_id", None),
            }
