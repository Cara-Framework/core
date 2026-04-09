## Goal
Create `tests/validation/test_new_rules.py` with tests covering `MinLengthRule`, `MaxLengthRule`, `PhoneRule`, and `SlugRule`.

## Steps
1. Create `tests/validation/__init__.py` (empty).
2. Create `tests/validation/test_new_rules.py` using pytest style.
3. Import all four rule classes from `cara.validation.rules`.
4. Each rule is tested by instantiating it and calling `.validate(field, value, params)` directly. The `params` dict must include the rule key (e.g., `{"min_length": "5"}` for MinLengthRule).

### MinLengthRule (at least 3 assertions)
- `validate("name", "hello", {"min_length": "5"})` → `True` (exactly 5)
- `validate("name", "hi", {"min_length": "5"})` → `False` (too short)
- `validate("name", "hello world", {"min_length": "5"})` → `True` (longer)
- `validate("name", None, {"min_length": "5"})` → `False`
- `validate("name", 123, {"min_length": "2"})` → `False` (not a string)

### MaxLengthRule (at least 3 assertions)
- `validate("name", "hi", {"max_length": "5"})` → `True`
- `validate("name", "hello world", {"max_length": "5"})` → `False`
- `validate("name", "hello", {"max_length": "5"})` → `True` (exactly 5)
- `validate("name", None, {"max_length": "5"})` → `False`

### PhoneRule (at least 3 assertions)
- `validate("phone", "+1234567890", {})` → `True`
- `validate("phone", "1234567890", {})` → `False` (no +)
- `validate("phone", "+123", {})` → `False` (too short)
- `validate("phone", None, {})` → `False`

### SlugRule (at least 3 assertions)
- `validate("slug", "hello-world", {})` → `True`
- `validate("slug", "hello world", {})` → `False` (contains space)
- `validate("slug", "hello_world", {})` → `True`
- `validate("slug", None, {})` → `False`

5. Also test `default_message` (or `message`) method for each rule — verify it returns a non-empty string containing the field name.

## Files
- `tests/validation/__init__.py` (new, empty)
- `tests/validation/test_new_rules.py` (new)

## Reference Files
- `cara/validation/rules/MinLengthRule.py` (from task 04)
- `cara/validation/rules/MaxLengthRule.py` (from task 04)
- `cara/validation/rules/PhoneRule.py`
- `cara/validation/rules/SlugRule.py`
- `cara/validation/rules/BaseRule.py` (validate signature)