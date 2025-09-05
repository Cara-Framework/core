"""
Unified Pipeline System for Cara Framework.

Supports both command workflows and job chaining with topic exchange routing.
Laravel-inspired pipeline pattern with async support and priority routing.
"""

import asyncio
import uuid
from enum import Enum
from typing import Any, Callable, Dict, List

from cara.facades import Log


class PipelineType(Enum):
    """Pipeline execution types."""
    SYNC = "sync"           # Execute immediately (commands)
    ASYNC_CHAIN = "chain"   # Queue jobs in sequence
    ASYNC_PARALLEL = "parallel"  # Queue jobs in parallel


class PipelineStep:
    """Individual step in a pipeline."""
    
    def __init__(self, 
                 step_class,
                 args: tuple = (),
                 kwargs: dict = None,
                 routing_key: str = None,
                 priority: str = "default",
                 condition: Callable = None,
                 on_success: Callable = None,
                 on_failure: Callable = None):
        """
        Initialize pipeline step.
        
        Args:
            step_class: Command or Job class to execute
            args: Arguments to pass to step
            kwargs: Keyword arguments to pass to step  
            routing_key: Routing key for job dispatch (if job)
            priority: Priority level for routing
            condition: Optional condition function to determine if step should run
            on_success: Callback on step success
            on_failure: Callback on step failure
        """
        self.step_class = step_class
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.routing_key = routing_key
        self.priority = priority
        self.condition = condition
        self.on_success = on_success
        self.on_failure = on_failure
        
        # Auto-generate routing key for jobs
        if not self.routing_key and hasattr(step_class, '__name__'):
            class_name = step_class.__name__.lower()
            if 'job' in class_name:
                domain = class_name.replace('job', '').replace('product', 'product')
                self.routing_key = f"{domain}.{self.priority}"


class Pipeline:
    """
    Unified pipeline system for commands and jobs.
    
    Features:
    - Command workflows (sync execution)
    - Job chaining (async with routing keys)
    - Conditional steps
    - Error handling and retries
    - Progress tracking
    - Priority-based routing
    
    Usage:
        # Command workflow
        Pipeline.create(PipelineType.SYNC)\
            .add(SeedMarketplaces)\
            .add(SeedCategories)\
            .execute()
            
        # Job chain
        Pipeline.create(PipelineType.ASYNC_CHAIN)\
            .add(CollectProductsJob, "trending", priority="high")\
            .add(EnrichProductJob, priority="high")\
            .add(ValidateProductJob, priority="high")\
            .dispatch()
            
        # Parallel jobs
        Pipeline.create(PipelineType.ASYNC_PARALLEL)\
            .add(CollectProductsJob, "trending", priority="high")\
            .add(CollectProductsJob, "keyword", "iPhone", priority="high")\
            .dispatch()
    """
    
    def __init__(self, pipeline_type: PipelineType, name: str = None):
        """Initialize pipeline."""
        self.pipeline_type = pipeline_type
        self.name = name or f"pipeline_{uuid.uuid4().hex[:8]}"
        self.steps: List[PipelineStep] = []
        self.context: Dict[str, Any] = {}
        self.results: List[Dict[str, Any]] = []
        
    @classmethod
    def create(cls, pipeline_type: PipelineType, name: str = None) -> "Pipeline":
        """Create a new pipeline."""
        return cls(pipeline_type, name)
    
    def add(self, 
            step_class,
            *args,
            priority: str = "default",
            routing_key: str = None,
            condition: Callable = None,
            on_success: Callable = None,
            on_failure: Callable = None,
            **kwargs) -> "Pipeline":
        """
        Add a step to the pipeline.
        
        Args:
            step_class: Command or Job class
            *args: Arguments for the step
            priority: Priority level (critical, high, default, low)
            routing_key: Custom routing key (auto-generated if None)
            condition: Optional condition to check before executing step
            on_success: Callback on step success
            on_failure: Callback on step failure
            **kwargs: Keyword arguments for the step
        """
        step = PipelineStep(
            step_class=step_class,
            args=args,
            kwargs=kwargs,
            routing_key=routing_key,
            priority=priority,
            condition=condition,
            on_success=on_success,
            on_failure=on_failure
        )
        
        self.steps.append(step)
        return self
    
    def when(self, condition: Callable) -> "ConditionalPipeline":
        """Add conditional step."""
        return ConditionalPipeline(self, condition)
    
    def set_context(self, key: str, value: Any) -> "Pipeline":
        """Set context variable."""
        self.context[key] = value
        return self
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get context variable."""
        return self.context.get(key, default)
    
    async def execute(self) -> Dict[str, Any]:
        """Execute the pipeline based on type."""
        Log.info(f"🚀 Executing pipeline: {self.name} Type: {self.pipeline_type.value}", category="cara.pipeline")
        
        if self.pipeline_type == PipelineType.SYNC:
            return await self._execute_sync()
        elif self.pipeline_type == PipelineType.ASYNC_CHAIN:
            return await self._execute_async_chain()
        elif self.pipeline_type == PipelineType.ASYNC_PARALLEL:
            return await self._execute_async_parallel()
        else:
            raise ValueError(f"Unknown pipeline type: {self.pipeline_type}")
    
    def dispatch(self) -> Dict[str, Any]:
        """Dispatch async pipeline (non-blocking)."""
        if self.pipeline_type == PipelineType.SYNC:
            raise ValueError("Cannot dispatch sync pipeline. Use execute() instead.")
        
        Log.info(f"📡 Dispatching pipeline: {self.name} Type: {self.pipeline_type.value}", category="cara.pipeline")
        
        # For async pipelines, we queue the first job and let it chain
        if self.pipeline_type == PipelineType.ASYNC_CHAIN:
            return self._dispatch_chain()
        elif self.pipeline_type == PipelineType.ASYNC_PARALLEL:
            return self._dispatch_parallel()
    
    async def _execute_sync(self) -> Dict[str, Any]:
        """Execute pipeline synchronously (for commands)."""
        successful_steps = 0
        total_steps = len(self.steps)
        
        for i, step in enumerate(self.steps, 1):
            # Check condition
            if step.condition and not step.condition(self.context):
                Log.info(f"⏭️ Skipping step {i}: {step.step_class.__name__} (condition not met)", category="cara.pipeline")
                continue
            
            Log.info(f"🔄 Executing step {i}/{total_steps}: {step.step_class.__name__}", category="cara.pipeline")
            
            try:
                # Execute command
                instance = step.step_class()
                if hasattr(instance, 'handle'):
                    result = await self._safe_call(instance.handle, *step.args, **step.kwargs)
                else:
                    result = await self._safe_call(instance, *step.args, **step.kwargs)
                
                # Store result
                step_result = {
                    'step': step.step_class.__name__,
                    'success': True,
                    'result': result,
                    'index': i
                }
                self.results.append(step_result)
                successful_steps += 1
                
                # Call success callback
                if step.on_success:
                    step.on_success(step_result, self.context)
                
                Log.info(f"✅ Step completed: {step.step_class.__name__}", category="cara.pipeline")
                
            except Exception as e:
                step_result = {
                    'step': step.step_class.__name__,
                    'success': False,
                    'error': str(e),
                    'index': i
                }
                self.results.append(step_result)
                
                # Call failure callback
                if step.on_failure:
                    step.on_failure(step_result, self.context)
                
                Log.error(f"❌ Step failed: {step.step_class.__name__} - {str(e)}", category="cara.pipeline")
        
        success_rate = successful_steps / total_steps if total_steps > 0 else 0
        
        result = {
            'success': successful_steps == total_steps,
            'success_rate': success_rate,
            'successful_steps': successful_steps,
            'total_steps': total_steps,
            'results': self.results,
            'context': self.context,
            'pipeline_name': self.name,
            'pipeline_type': self.pipeline_type.value
        }
        
        Log.info(f"🏁 Pipeline completed: {self.name} Success: {successful_steps}/{total_steps}", category="cara.pipeline")
        return result
    
    async def _execute_async_chain(self) -> Dict[str, Any]:
        """Execute pipeline as async chain (sequential job execution)."""
        if not self.steps:
            return {'success': False, 'error': 'No steps to execute'}
        
        Log.info(f"🔗 Executing async chain: {len(self.steps)} steps", category="cara.pipeline")
        
        successful_steps = 0
        total_steps = len(self.steps)
        
        for i, step in enumerate(self.steps, 1):
            # Check condition
            if step.condition and not step.condition(self.context):
                Log.info(f"⏭️ Skipping step {i}: {step.step_class.__name__} (condition not met)", category="cara.pipeline")
                continue
            
            Log.info(f"🔄 Dispatching step {i}/{total_steps}: {step.step_class.__name__}", category="cara.pipeline")
            
            try:
                # Check if it's a job class with dispatch method
                if hasattr(step.step_class, 'dispatch'):
                    job_dispatch = step.step_class.dispatch(*step.args, **step.kwargs)
                    
                    if step.routing_key:
                        job_id = job_dispatch.withRoutingKey(step.routing_key)
                    else:
                        job_id = job_dispatch
                    
                    # Store result
                    step_result = {
                        'step': step.step_class.__name__,
                        'success': True,
                        'job_id': job_id,
                        'routing_key': step.routing_key,
                        'priority': step.priority,
                        'index': i
                    }
                    self.results.append(step_result)
                    successful_steps += 1
                    
                    # Call success callback
                    if step.on_success:
                        step.on_success(step_result, self.context)
                    
                    Log.info(f"✅ Step dispatched: {step.step_class.__name__} [{job_id}]", category="cara.pipeline")
                else:
                    raise ValueError(f"Step {step.step_class.__name__} is not a dispatchable job")
                    
            except Exception as e:
                step_result = {
                    'step': step.step_class.__name__,
                    'success': False,
                    'error': str(e),
                    'index': i
                }
                self.results.append(step_result)
                
                # Call failure callback
                if step.on_failure:
                    step.on_failure(step_result, self.context)
                
                Log.error(f"❌ Step dispatch failed: {step.step_class.__name__} - {str(e)}", category="cara.pipeline")
        
        success_rate = successful_steps / total_steps if total_steps > 0 else 0
        
        result = {
            'success': successful_steps == total_steps,
            'success_rate': success_rate,
            'successful_steps': successful_steps,
            'total_steps': total_steps,
            'results': self.results,
            'context': self.context,
            'pipeline_name': self.name,
            'pipeline_type': self.pipeline_type.value
        }
        
        Log.info(f"🏁 Async chain completed: {self.name} Success: {successful_steps}/{total_steps}", category="cara.pipeline")
        return result
    
    async def _execute_async_parallel(self) -> Dict[str, Any]:
        """Execute pipeline as async parallel (parallel job execution)."""
        return self._dispatch_parallel()
    
    def _dispatch_chain(self) -> Dict[str, Any]:
        """Dispatch job chain (first job will chain to next)."""
        if not self.steps:
            return {'success': False, 'error': 'No steps to dispatch'}
        
        # For now, dispatch first job with pipeline context
        # TODO: Implement proper chaining mechanism
        first_step = self.steps[0]
        
        try:
            # Check if it's a job class with dispatch method
            if hasattr(first_step.step_class, 'dispatch'):
                job_dispatch = first_step.step_class.dispatch(*first_step.args, **first_step.kwargs)
                
                if first_step.routing_key:
                    job_id = job_dispatch.withRoutingKey(first_step.routing_key)
                else:
                    job_id = job_dispatch
                
                Log.info(f"🔗 Chain started: {first_step.step_class.__name__} [{job_id}]", category="cara.pipeline")
                
                return {
                    'success': True,
                    'chain_started': True,
                    'first_job_id': job_id,
                    'pipeline_name': self.name,
                    'total_steps': len(self.steps)
                }
            else:
                raise ValueError(f"Step {first_step.step_class.__name__} is not a dispatchable job")
                
        except Exception as e:
            Log.error(f"❌ Chain dispatch failed: {str(e)}", category="cara.pipeline")
            return {'success': False, 'error': str(e)}
    
    def _dispatch_parallel(self) -> Dict[str, Any]:
        """Dispatch jobs in parallel."""
        job_ids = []
        
        for step in self.steps:
            try:
                if hasattr(step.step_class, 'dispatch'):
                    job_dispatch = step.step_class.dispatch(*step.args, **step.kwargs)
                    
                    if step.routing_key:
                        job_id = job_dispatch.withRoutingKey(step.routing_key)
                    else:
                        job_id = job_dispatch
                    
                    job_ids.append({
                        'job': step.step_class.__name__,
                        'job_id': job_id,
                        'routing_key': step.routing_key,
                        'priority': step.priority
                    })
                    
                    Log.info(f"📡 Dispatched: {step.step_class.__name__} [{job_id}]", category="cara.pipeline")
                    
            except Exception as e:
                Log.error(f"❌ Failed to dispatch {step.step_class.__name__}: {str(e)}", category="cara.pipeline")
        
        Log.info(f"🚀 Parallel dispatch completed: {len(job_ids)} jobs", category="cara.pipeline")
        
        return {
            'success': len(job_ids) > 0,
            'jobs_dispatched': len(job_ids),
            'job_ids': job_ids,
            'pipeline_name': self.name,
            'pipeline_type': self.pipeline_type.value
        }
    
    async def _safe_call(self, func, *args, **kwargs):
        """Safely call a function (sync or async)."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            return func(*args, **kwargs)


class ConditionalPipeline:
    """Helper for conditional pipeline steps."""
    
    def __init__(self, pipeline: Pipeline, condition: Callable):
        self.pipeline = pipeline
        self.condition = condition
    
    def add(self, step_class, *args, **kwargs) -> Pipeline:
        """Add conditional step."""
        return self.pipeline.add(step_class, *args, condition=self.condition, **kwargs) 