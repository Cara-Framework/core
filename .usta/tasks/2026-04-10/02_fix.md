# Fix: Check if event propagation has been stopped

## Reviewer Summary
has_listeners, silent no-listener dispatch, and propagation stop check are all correct

## Issues Found
- [warning] cara/events/Event.py:20 — [pyflakes] 1 'cara.exceptions.ListenerNotFoundException' imported but unused

## Fix Instructions
Static checker [pyflakes] flagged 1 issue(s) on the files this task touched. Resolve them without breaking other behaviour:
- cara/events/Event.py:20 1 'cara.exceptions.ListenerNotFoundException' imported but unused