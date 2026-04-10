# Fix: --- Registration and dispatching ---

## Reviewer Summary
pytest: 0 failing tests; Multiple tests will fail because Event.dispatch lacks propagation-stop logic, no-listener handling, and has_listeners method

## Issues Found
- [error] tests/events/test_event_dispatcher.py:77 — test_dispatch_no_listeners_is_noop will fail: dispatch() raises ListenerNotFoundException when no listeners are registered
- [error] tests/events/test_event_dispatcher.py:100 — test_stop_propagation will fail: dispatch() never checks is_propagation_stopped so AfterStopListener.handle will be called
- [error] tests/events/test_event_dispatcher.py:149 — test_has_listeners_true/false/wildcard will fail: Event class has no has_listeners method

## Fix Instructions
Test runner [pytest] reported 0 failing of 1 executed tests on the files this task touched. Fix the code so every test passes. Failures:


Last runner output:

==================================== ERRORS ====================================
____________ ERROR collecting tests/events/test_event_dispatcher.py ____________
ImportError while importing test module '/Users/cfkarakulak/Desktop/cheapa.io/code/commons/cara/tests/events/test_event_dispatcher.py'.
Hint: make sure your test modules/packages have valid Python names.
Traceback:
/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/lib/python3.14/importlib/__init__.py:88: in import_module
    return _bootstrap._gcd_import(name[level:], package, level)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/events/test_event_dispatcher.py:4: in <module>
    from cara.events.Event import Event as EventDispatcher, EventSubscriber
cara/events/__init__.py:1: in <module>
    from .Event import Event
cara/events/Event.py:22: in <module>
    from cara.queues.contracts import ShouldQueue
cara/queues/__init__.py:3: in <module>
    from .QueueProvider import QueueProvider
cara/queues/QueueProvider.py:14: in <module>
    from cara.queues.drivers import AMQPDriver, AsyncDriver, DatabaseDriver, RedisDriver
cara/queues/drivers/__init__.py:1: in <module>
    from .AMQPDriver import AMQPDriver
cara/queues/drivers/AMQPDriver.py:14: in <module>
    import pika
E   ModuleNotFoundError: No module named 'pika'
=========================== short test summary info ============================
ERROR tests/events/test_event_dispatcher.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.11s


The tests themselves are correct — they test the intended behavior. The fixes must be applied to cara/events/Event.py as described in Task 02's fix_instructions:
1. Replace the ListenerNotFoundException raise (lines 205-208) with `return` when no listeners found.
2. Add `if hasattr(event, 'is_propagation_stopped') and event.is_propagation_stopped: break` at the top of the `for listener in all_listeners:` loop.
3. Add a `has_listeners(self, event_name: str) -> bool` method that checks `self._listeners` for direct matches and `self._get_matching_wildcard_listeners(event_name)` for wildcard matches.