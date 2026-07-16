"""
Unified Pipeline System for Cara Framework.

Supports synchronous command workflows. The legacy async chain and parallel
types fail closed until durable signed orchestration descriptors exist.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Callable
from enum import Enum
from typing import Any

from cara.exceptions import InvalidArgumentException, QueueException
from cara.facades import Log


class StepFailed(Exception):
    """A pipeline step signalled failure via a non-zero exit code.

    Raised internally so a step whose ``handle()`` RETURNS a non-zero
    craft exit code flows into the same failure path as one that raised —
    it must not be counted as a completed step.
    """


class PipelineType(Enum):
    """Pipeline execution types."""

    SYNC = "sync"  # Execute immediately (commands)
    ASYNC_CHAIN = "chain"  # Reserved; dispatch fails closed
    ASYNC_PARALLEL = "parallel"  # Reserved; dispatch fails closed


class PipelineStep:
    """Individual step in a pipeline."""

    def __init__(
        self,
        step_class,
        args: tuple = (),
        kwargs: dict | None = None,
        routing_key: str | None = None,
        priority: str = "default",
        condition: Callable | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
    ):
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
        if not self.routing_key and hasattr(step_class, "__name__"):
            class_name = step_class.__name__.lower()
            if "job" in class_name:
                domain = class_name.replace("job", "")
                self.routing_key = f"{domain}.{self.priority}"


class Pipeline:
    """
    Unified pipeline system for commands and jobs.

    Features:
    - Command workflows (sync execution)
    - Conditional steps
    - Error handling
    - Progress tracking

    Usage:
        # Command workflow
        Pipeline.create(PipelineType.SYNC)\
            .add(SeedMarketplaces)\
            .add(SeedCategories)\
            .execute()

    ``ASYNC_CHAIN`` and ``ASYNC_PARALLEL`` remain reserved enum values; using
    either raises ``QueueException`` instead of silently dropping work.
    """

    def __init__(self, pipeline_type: PipelineType, name: str | None = None):
        """Initialize pipeline."""
        self.pipeline_type = pipeline_type
        self.name = name or f"pipeline_{uuid.uuid4().hex[:8]}"
        self.steps: list[PipelineStep] = []
        self.context: dict[str, Any] = {}
        self.results: list[dict[str, Any]] = []

    @classmethod
    def create(cls, pipeline_type: PipelineType, name: str | None = None) -> Pipeline:
        """Create a new pipeline."""
        return cls(pipeline_type, name)

    def add(
        self,
        step_class,
        *args,
        priority: str = "default",
        routing_key: str | None = None,
        condition: Callable | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
        **kwargs,
    ) -> Pipeline:
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
            on_failure=on_failure,
        )

        self.steps.append(step)
        return self

    def when(self, condition: Callable) -> ConditionalPipeline:
        """Add conditional step."""
        return ConditionalPipeline(self, condition)

    def set_context(self, key: str, value: Any) -> Pipeline:
        """Set context variable."""
        self.context[key] = value
        return self

    def get_context(self, key: str, default: Any = None) -> Any:
        """Get context variable."""
        return self.context.get(key, default)

    async def execute(self) -> dict[str, Any]:
        """Execute the pipeline based on type."""
        Log.info("🚀 Executing pipeline: %s Type: %s", self.name, self.pipeline_type.value, category='cara.pipeline')

        if self.pipeline_type == PipelineType.SYNC:
            return await self._execute_sync()
        elif self.pipeline_type == PipelineType.ASYNC_CHAIN:
            return await self._execute_async_chain()
        elif self.pipeline_type == PipelineType.ASYNC_PARALLEL:
            return await self._execute_async_parallel()
        else:
            raise InvalidArgumentException(f"Unknown pipeline type: {self.pipeline_type}")

    def dispatch(self) -> dict[str, Any]:
        """Dispatch async pipeline (non-blocking)."""
        if self.pipeline_type == PipelineType.SYNC:
            raise InvalidArgumentException("Cannot dispatch sync pipeline. Use execute() instead.")

        Log.info("📡 Dispatching pipeline: %s Type: %s", self.name, self.pipeline_type.value, category='cara.pipeline')

        # Reserved async modes fail closed in their dispatch helpers.
        if self.pipeline_type == PipelineType.ASYNC_CHAIN:
            return self._dispatch_chain()
        elif self.pipeline_type == PipelineType.ASYNC_PARALLEL:
            return self._dispatch_parallel()

    async def _execute_sync(self) -> dict[str, Any]:
        """Execute pipeline synchronously (for commands)."""
        successful_steps = 0
        skipped_steps = 0
        total_steps = len(self.steps)

        for i, step in enumerate(self.steps, 1):
            # Check condition
            if step.condition and not step.condition(self.context):
                skipped_steps += 1
                Log.info("⏭️ Skipping step %s: %s (condition not met)", i, step.step_class.__name__, category='cara.pipeline')
                continue

            Log.info("🔄 Executing step %s/%s: %s", i, total_steps, step.step_class.__name__, category='cara.pipeline')

            try:
                # Execute command - instantiate with constructor args only
                instance = step.step_class(*step.args)

                # Pass kwargs to handle() method (for flags like --existing).
                # Use the cara IoC container's ``application.call`` when the
                # step is a CommandBase, so contract-typed parameters
                # (``seed: SeedDataContract``, etc.) get auto-injected the
                # same way they are when the step is run as a stand-alone
                # craft command — pipelines should not regress that.
                application = getattr(instance, "application", None)
                if hasattr(instance, "handle"):
                    target = instance.handle
                    if application is not None and hasattr(application, "call"):
                        if asyncio.iscoroutinefunction(target):
                            result = await application.call(target, **step.kwargs)
                        else:
                            result = application.call(target, **step.kwargs)
                    else:
                        result = await self._safe_call(target, **step.kwargs)
                else:
                    result = await self._safe_call(instance, **step.kwargs)

                # Craft commands signal failure by RETURNING a non-zero exit
                # code (the Typer/CLI convention used throughout), not by
                # raising. A step whose handle() returns e.g. 1 has FAILED —
                # counting it as success let a silently-failed seed report
                # "N/N completed" and mask the failure. Treat a non-zero int
                # result as a failed step. (Non-int results — dicts, domain
                # objects, None — are not exit codes and stay successful.)
                if isinstance(result, int) and result != 0:
                    raise StepFailed(
                        f"{step.step_class.__name__} returned exit code {result}"
                    )

                # Store result
                step_result = {
                    "step": step.step_class.__name__,
                    "success": True,
                    "result": result,
                    "index": i,
                }
                self.results.append(step_result)
                successful_steps += 1

                # Call success callback
                if step.on_success:
                    step.on_success(step_result, self.context)

                Log.info("✅ Step completed: %s", step.step_class.__name__, category='cara.pipeline')

            except Exception as e:
                step_result = {
                    "step": step.step_class.__name__,
                    "success": False,
                    "error": str(e),
                    "index": i,
                }
                self.results.append(step_result)

                # Call failure callback
                if step.on_failure:
                    step.on_failure(step_result, self.context)

                Log.error("❌ Step failed: %s - %s", step.step_class.__name__, str(e), category='cara.pipeline')

        # Condition-skipped steps are not failures — a healthy run with a
        # legitimately-skipped optional step must still report success.
        attempted_steps = total_steps - skipped_steps
        success_rate = (
            successful_steps / attempted_steps if attempted_steps > 0 else 1.0
        )

        result = {
            "success": successful_steps == attempted_steps,
            "success_rate": success_rate,
            "successful_steps": successful_steps,
            "skipped_steps": skipped_steps,
            "total_steps": total_steps,
            "results": self.results,
            "context": self.context,
            "pipeline_name": self.name,
            "pipeline_type": self.pipeline_type.value,
        }

        Log.info("🏁 Pipeline completed: %s Success: %s/%s (%s skipped)", self.name, successful_steps, attempted_steps, skipped_steps, category='cara.pipeline')
        return result

    async def _execute_async_chain(self) -> dict[str, Any]:
        """Validate a reserved async chain request before rejecting dispatch."""
        if not self.steps:
            return {"success": False, "error": "No steps to execute"}

        # Dispatch-time conditions still apply — steps whose condition
        # rejects the current context are excluded from the chain.
        runnable = [
            step
            for step in self.steps
            if not (step.condition and not step.condition(self.context))
        ]
        skipped = len(self.steps) - len(runnable)

        Log.info("🔗 Executing async chain: %s steps (%s skipped)", len(runnable), skipped, category='cara.pipeline')

        result = self._dispatch_chain(runnable)
        result.update(
            {
                "skipped_steps": skipped,
                "context": self.context,
                "pipeline_type": self.pipeline_type.value,
            }
        )
        return result

    async def _execute_async_parallel(self) -> dict[str, Any]:
        """Execute pipeline as async parallel (parallel job execution)."""
        return self._dispatch_parallel()

    def _dispatch_chain(self, steps: list[PipelineStep] | None = None) -> dict[str, Any]:
        raise QueueException(
            "Async pipeline chains are unsupported until durable JSON "
            "chain descriptors are implemented."
        )

    def _dispatch_parallel(self) -> dict[str, Any]:
        raise QueueException(
            "Async parallel pipelines are unsupported until durable JSON "
            "batch descriptors are implemented."
        )

    async def _safe_call(self, func, *args, **kwargs):
        """Safely call a function (sync or async)."""
        if inspect.iscoroutinefunction(func):
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
