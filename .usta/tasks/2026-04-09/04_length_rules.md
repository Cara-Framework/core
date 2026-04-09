## Goal
Create two new validation rules — `MinLengthRule` and `MaxLengthRule` — that validate string length specifically (unlike the existing `MinRule`/`MaxRule` which handle both numeric and length). Register them in the rules `__init__.py`.

## Steps

### MinLengthRule.py (new file)
1. Create `cara/validation/rules/MinLengthRule.py` following the `EmailRule` pattern.
2. Module-level docstring: `"""Min Length Validation Rule for the Cara framework.\n\nThis module provides a validation rule that checks if a string value meets a minimum length."""`
3. Import `Any, Dict` from typing, `MessageFormatter` from `cara.validation`, and `BaseRule` from `cara.validation.rules`.
4. Class `MinLengthRule(BaseRule)` with docstring: `"""Validates that a string has at least a given number of characters.\n\nUsage: \"min_length:5\""""`
5. `validate(self, field, value, params)` method:
   - Return `False` if value is `None` or not a `str`.
   - Read `params.get("min_length")`. Return `False` if missing.
   - Convert param to `int`. Return `len(value) >= threshold`.
6. `default_message(self, field, params)` method:
   - Use `MessageFormatter.format_attribute_name(field)` for the attribute name.
   - Return: `f"The {attribute.lower()} field must be at least {min_val} characters long."`

### MaxLengthRule.py (new file)
7. Create `cara/validation/rules/MaxLengthRule.py` with identical structure.
8. Class `MaxLengthRule(BaseRule)`, usage `"max_length:255"`.
9. `validate`: Return `False` if value is `None` or not `str`. Check `len(value) <= threshold`.
10. `default_message`: `f"The {attribute.lower()} field must not exceed {max_val} characters."`

### Register in __init__.py
11. In `cara/validation/rules/__init__.py`, add imports for both new rules (alphabetical order, after `MaxRule` import):
    ```python
    from .MaxLengthRule import MaxLengthRule
    from .MinLengthRule import MinLengthRule
    ```
12. Add `"MaxLengthRule"` and `"MinLengthRule"` to the `__all__` list in alphabetical position.

## Files
- `cara/validation/rules/MinLengthRule.py` (new)
- `cara/validation/rules/MaxLengthRule.py` (new)
- `cara/validation/rules/__init__.py`

## Reference Files
- `cara/validation/rules/EmailRule.py` (pattern to follow)
- `cara/validation/rules/BaseRule.py` (base class)
- `cara/validation/MessageFormatter.py` (MessageFormatter usage)