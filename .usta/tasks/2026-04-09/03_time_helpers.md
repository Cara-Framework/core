## Goal
Add `humanize_seconds` and `format_duration` helpers to `cara/support/Time.py` and export them from `cara/support/__init__.py`.

## Steps

### Time.py changes
1. Open `cara/support/Time.py`. Add two new functions **after** the existing `migration_timestamp` function. Keep all existing code intact.
2. Implement `humanize_seconds(seconds: int) -> str`:
   - Convert an integer number of seconds into a human-readable string.
   - Break down into days, hours, minutes, seconds.
   - Only include non-zero components. Example: `humanize_seconds(273132)` → `"3 days 4 hours 12 minutes 12 seconds"`.
   - Use singular form when value is 1 ("1 day", "1 hour", "1 minute", "1 second").
   - Return `"0 seconds"` for input 0.
   - Does NOT need to import pendulum — pure arithmetic.
3. Implement `format_duration(seconds: int) -> str`:
   - Compact format: `"1h 23m 45s"`.
   - Only include non-zero components. `format_duration(45)` → `"45s"`, `format_duration(3661)` → `"1h 1m 1s"`.
   - Return `"0s"` for input 0.
   - Pure arithmetic, no pendulum needed.
4. Both functions must have type hints and docstrings.

### __init__.py changes
5. In `cara/support/__init__.py`, add a new import line:
   ```python
   from .Time import humanize_seconds, format_duration
   ```
6. Add both names to `__all__`, in a new `# Time utilities` comment section.

## Files
- `cara/support/Time.py`
- `cara/support/__init__.py`

## Reference Files
- `cara/support/Str.py` (for docstring style reference)