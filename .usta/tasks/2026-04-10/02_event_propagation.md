## Add propagation-stopping support to the Event dispatcher

Update the `dispatch()` method in `cara/events/Event.py` to check `event.is_propagation_stopped` after each listener and break out of the loop if it returns True. This is a small, backwards-compatible change: events without `is_propagation_stopped` are unaffected.

Also add a `has_listeners()` method and make `dispatch()` silently return (instead of raising `ListenerNotFoundException`) when no listeners exist, matching Laravel behavior where dispatching an event with no listeners is a no-op.

### cara/events/Event.py

In the `dispatch()` method, after calling each listener, add:

```python
# Check if event propagation has been stopped
if hasattr(event, 'is_propagation_stopped') and event.is_propagation_stopped:
    break
```

Also replace the `ListenerNotFoundException` raise block with a simple `return`.

Add a new `has_listeners()` method:

```python
def has_listeners(self, event_name: str) -> bool:
    """Check if any listeners are registered for the given event name."""
    if event_name in self._listeners and self._listeners[event_name]:
        return True
    if self._get_matching_wildcard_listeners(event_name):
        return True
    return False
```

The full updated `dispatch()` method should be:

```python
async def dispatch(self, event) -> None:
    # Get event name - support both property and method
    if hasattr(event, "name"):
        event_name = event.name if isinstance(event.name, str) else event.name()
    else:
        event_name = event.__class__.__name__.lower()

    # Get direct listeners for this event name
    direct_listeners = self._listeners.get(event_name, []).copy()

    # Get wildcard listeners that match this event name
    wildcard_listeners = self._get_matching_wildcard_listeners(event_name)

    # Combine all listeners
    all_listeners = direct_listeners + wildcard_listeners

    if not all_listeners:
        return

    # Check if we're in sync mode
    from cara.context import ExecutionContext

    is_sync = ExecutionContext.is_sync()

    for listener in all_listeners:
        # Laravel-style queue check: If listener implements ShouldQueue, queue it
        if self._should_queue(listener):
            self._queue_listener(listener, event)
        else:
            # Execute immediately - handle both sync and async listeners
            if inspect.iscoroutinefunction(listener.handle):
                if is_sync:
                    await listener.handle(event)
                else:
                    asyncio.create_task(listener.handle(event))
            else:
                listener.handle(event)

        # Check if event propagation has been stopped
        if hasattr(event, 'is_propagation_stopped') and event.is_propagation_stopped:
            break
```

## Files
- cara/events/Event.py
