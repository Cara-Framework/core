# Queue Features: UniqueJob & Middleware

## 1. UniqueJob Contract

Prevents duplicate job dispatching by tracking unique identifiers and acquiring locks.

### Usage Example

```python
from cara.queues.contracts import ShouldQueue, Queueable, UniqueJob

class RefreshProductJob(ShouldQueue, Queueable, UniqueJob):
    def __init__(self, product_id: int):
        self.product_id = product_id
    
    def unique_id(self) -> str:
        """Return unique identifier for this job instance."""
        return f"refresh_product_{self.product_id}"
    
    @property
    def unique_for(self) -> int:
        """Lock duration in seconds. Default: 3600 (1 hour)."""
        return 1800  # 30 minutes
    
    async def handle(self):
        # Refresh product...
        pass

# Dispatch logic:
# 1st dispatch → Acquired lock, queued for processing
# 2nd dispatch (same product_id) → Silently dropped (already locked)
# After 30 minutes (or on completion) → Lock released
```

### How It Works

1. **On Dispatch (Bus.dispatch)**: 
   - Checks if job implements `UniqueJob`
   - Gets unique_id and checks `is_unique_locked()`
   - If locked: silently drops dispatch (returns None)
   - If not locked: acquires lock with `unique_for` TTL

2. **On Completion (QueueWorkCommand.process_message)**:
   - After successful execution: releases lock
   - On timeout error: releases lock
   - On general error: releases lock

### Lock Storage

- **Single-process**: In-memory thread-safe dict with TTL support
- **Production**: Should extend to Redis for multi-worker scenarios

### API

```python
# Check if locked
if UniqueJob.is_unique_locked(unique_id: str) -> bool

# Acquire lock
UniqueJob.acquire_unique_lock(unique_id: str, ttl: int = 3600) -> bool

# Release lock
UniqueJob.release_unique_lock(unique_id: str) -> None

# Cleanup expired locks
UniqueJob.cleanup_expired_locks() -> int  # Returns count removed
```

---

## 2. Job Middleware System

Allows jobs to define middleware pipeline for execution control (rate limiting, overlap prevention, etc).

### Available Middleware

#### RateLimited
Limits job execution frequency within a time window.

```python
from cara.queues.middleware.RateLimited import RateLimited

class SyncDataJob(ShouldQueue, Queueable):
    def middleware(self):
        # Max 10 executions per 60 seconds
        return [RateLimited(max_attempts=10, decay_seconds=60)]
    
    async def handle(self):
        # Sync...
        pass

# Behavior:
# Execution 1-10 → Allowed
# Execution 11 (within 60s) → Rate limited, skipped, logged
# After 60s → Counter resets
```

#### WithoutOverlapping
Prevents concurrent execution of the same job type.

```python
from cara.queues.middleware.RateLimited import WithoutOverlapping

class HeavyProcessingJob(ShouldQueue, Queueable):
    def middleware(self):
        # Prevent overlapping execution, expire lock after 5 minutes
        return [WithoutOverlapping(key="heavy-process", expire_after=300)]
    
    async def handle(self):
        # Heavy processing...
        pass

# Behavior:
# Execution 1 → Lock acquired, processing
# Execution 2 (while locked) → Skipped, logged
# After job completes or 5 min expires → Lock released
```

### Custom Middleware

Create custom middleware by implementing the `handle(job, next_fn)` pattern:

```python
class MyCustomMiddleware:
    def __init__(self, config):
        self.config = config
    
    def handle(self, job, next_fn):
        """
        Execute middleware logic.
        
        Args:
            job: The job instance
            next_fn: Callable to continue pipeline
        
        Returns:
            Result from next_fn (or None to skip)
        """
        # Pre-processing
        print(f"Processing job: {job.__class__.__name__}")
        
        # Call next middleware or actual job
        result = next_fn(job)
        
        # Post-processing
        print(f"Completed job: {job.__class__.__name__}")
        
        return result

class MyJob(ShouldQueue, Queueable):
    def middleware(self):
        return [MyCustomMiddleware({"option": "value"})]
    
    async def handle(self):
        # Job logic...
        pass
```

### Middleware Pipeline Execution

Middlewares are applied in **reverse order** to create proper nesting:

```python
def middleware(self):
    return [MiddlewareA(), MiddlewareB(), MiddlewareC()]

# Execution flow:
# MiddlewareA.handle() → 
#   MiddlewareB.handle() → 
#     MiddlewareC.handle() → 
#       job.handle()
```

### Integration Point

Middleware is executed in `JobProcessor.process_message()` before the actual job handler is called:

1. Extract middlewares from `job.middleware()` if defined
2. Build pipeline (reverse order)
3. Execute pipeline with job instance
4. Continue with normal job lifecycle (tracking, ACK, etc)

---

## Implementation Details

### Files Created

1. **UniqueJob.py** - Contract for unique job deduplication
   - In-memory lock storage with TTL
   - Thread-safe operations
   - Exportable from `cara.queues.contracts`

2. **middleware/__init__.py** - Middleware package

3. **middleware/RateLimited.py** - Rate limiting & overlap prevention
   - `RateLimited`: Frequency-based rate limiting
   - `WithoutOverlapping`: Execution overlap prevention

### Files Modified

1. **queues/contracts/__init__.py** - Added UniqueJob export

2. **queues/Bus.py** - Added UniqueJob check in dispatch
   - Checks `is_unique_locked()` before queuing
   - Acquires lock on dispatch
   - Silent drop if already locked

3. **commands/core/QueueWorkCommand.py** - Added:
   - Import for UniqueJob
   - Lock release on success
   - Lock release on timeout
   - Lock release on failure

---

## Testing Considerations

```python
# Test UniqueJob deduplication
def test_unique_job_prevents_duplicate_dispatch():
    job = RefreshProductJob(product_id=123)
    
    # First dispatch succeeds
    result1 = await Bus.dispatch(job)
    assert result1 is None  # Queued
    
    # Second dispatch (same ID) is silently dropped
    result2 = await Bus.dispatch(job)
    assert result2 is None  # Still None, but dropped
    
    # After lock expires or job completes, new dispatch succeeds
    # ...

# Test middleware prevents concurrent execution
def test_without_overlapping_blocks_concurrent():
    job = HeavyProcessingJob()
    
    # First execution
    # Second execution (while first running) → Skipped
    # ...
```

---

## Future Enhancements

1. **Redis-backed locks** for multi-worker scenarios
   - Replace in-memory dict with Redis
   - Use SETEX for atomic lock + TTL

2. **Distributed locks** with Redlock algorithm
   - For multi-server queue workers

3. **Middleware hooks** system
   - `before_dispatch()`, `after_completion()`, etc.

4. **Job chaining** with middleware support
   - Chain multiple jobs with shared middleware

5. **Metrics collection**
   - Track rate limit violations, overlap blocks, etc.
