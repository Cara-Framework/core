# Fix: Check if event propagation has been stopped

## Reviewer Summary
Docstring updated but dispatch logic not changed: still raises on no listeners, never checks propagation stop

## Issues Found
- [error] cara/events/Event.py:206 — dispatch() still raises ListenerNotFoundException when no listeners match, contradicting the updated docstring and the test_dispatch_no_listeners_is_noop test
- [error] cara/events/Event.py:215 — dispatch() loop never checks event.is_propagation_stopped after calling each listener, so stop_propagation has no effect despite the docstring claiming it does

## Fix Instructions
In cara/events/Event.py:
1. Remove the ListenerNotFoundException raise block (lines 205-208). Replace it with a simple `return` so dispatching with no listeners is a no-op:
   
   if not all_listeners:
       return
   
2. Inside the `for listener in all_listeners:` loop (line 215), add a propagation-stopped check at the top of each iteration. Add these lines immediately after `for listener in all_listeners:`:
   
   if hasattr(event, 'is_propagation_stopped') and event.is_propagation_stopped:
       break
   
3. Add a `has_listeners` method to the Event class. Place it after the `listen` method (after line 171). It should check both direct listeners and wildcard listeners:
   python
   def has_listeners(self, event_name: str) -> bool:
       if event_name in self._listeners and self._listeners[event_name]:
           return True
       if self._get_matching_wildcard_listeners(event_name):
           return True
       return False
   