## Goal
Export the five new string helpers (`truncate`, `title_case`, `snake_case`, `kebab_case`, `camel_case`) from `cara/support/__init__.py`.

## Steps
1. In `cara/support/__init__.py`, update the import line from `.Str` to include the new functions:
   ```python
   from .Str import slugify, normalize_email, format_money, truncate, title_case, snake_case, kebab_case, camel_case
   ```
2. Add all five names to the `__all__` list, in the `# String utilities` section, after `"format_money"`:
   ```python
   "truncate", "title_case", "snake_case", "kebab_case", "camel_case",
   ```
3. Preserve all other existing imports and exports exactly as-is.

## Files
- `cara/support/__init__.py`

## Reference Files
- `cara/support/Str.py` (to verify function names)