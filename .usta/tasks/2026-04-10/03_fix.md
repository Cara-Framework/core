# Fix: --- Registration and dispatching ---

## Reviewer Summary
pytest: 0 failing tests; MagicMock event causes propagation check to falsely trigger, breaking listener ordering test

## Issues Found
- [error] tests/events/test_event_dispatcher.py:88 — test_listeners_called_in_registration_order uses MagicMock() as the event. MagicMock auto-creates attributes, so hasattr(event, 'is_propagation_stopped') is True and event.is_propagation_stopped returns a truthy MagicMock. The dispatch loop will break before any listener runs, making log == [] instead of ['first', 'second', 'third']. Test will fail.

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
1 error in 0.10s


In tests/events/test_event_dispatcher.py, in test_listeners_called_in_registration_order (around line 88), replace the MagicMock with a proper event object that has is_propagation_stopped = False. Either: (1) add `event.is_propagation_stopped = False` after creating the MagicMock, or (2) replace `event = MagicMock()` and `event.name = "order.test"` with a real event class. Option 1 is simplest: change lines 89-90 to:

    event = MagicMock()
    event.name = "order.test"
    event.is_propagation_stopped = False