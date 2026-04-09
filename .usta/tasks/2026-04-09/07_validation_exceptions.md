## Goal
Add a module-level docstring (already present) and an `__all__` list to `cara/exceptions/types/validation.py`.

## Steps
1. Open `cara/exceptions/types/validation.py`. It already has a module-level docstring — keep it exactly as-is.
2. Add an `__all__` list immediately after the module-level docstring and before the imports. The list should contain all three public classes defined in the file:
   ```python
   __all__ = [
       "ValidationException",
       "RuleNotFoundException",
       "InvalidRuleFormatException",
   ]
   ```
3. Do NOT modify any class definitions, imports, or the existing docstring. Only insert the `__all__` block.

## Files
- `cara/exceptions/types/validation.py`

## Reference Files
- `cara/validation/rules/__init__.py` (for `__all__` style reference)