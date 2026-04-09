## Goal
Create `tests/support/test_str.py` with comprehensive tests for all string helpers: `slugify`, `normalize_email`, `format_money`, `truncate`, `title_case`, `snake_case`, `kebab_case`, `camel_case`.

## Steps
1. Create directory structure: `tests/` and `tests/support/` with `__init__.py` files in each (empty files).
2. Create `tests/support/test_str.py` using `pytest` style (plain functions, no unittest class needed).
3. Import all helpers from `cara.support.Str` directly (e.g., `from cara.support.Str import slugify, ...`).
4. Write at least 3 assertions per helper function:

### slugify
- `slugify("Hello World")` == `"hello-world"`
- `slugify("  ")` == `""`
- `slugify("Héllo Wörld")` == `"hello-world"` (accented chars)
- `slugify("Hello World", "_")` == `"hello_world"` (custom separator)

### normalize_email
- `normalize_email(" Alice@Example.COM ")` == `"alice@example.com"`
- `normalize_email("")` == `""`
- `normalize_email("  ")` == `""`

### format_money
- `format_money(1050)` == `"$10.50"`
- `format_money(0)` == `"$0.00"`
- `format_money(100000, "EUR")` == `"€1,000.00"`
- Test `TypeError` raised for non-int input using `pytest.raises`.
- Test `ValueError` raised for negative cents.

### truncate
- `truncate("Hello World", 5)` == `"Hello..."`
- `truncate("Hi", 10)` == `"Hi"`
- `truncate("Hello", 3, "!")` == `"Hel!"`
- `truncate("", 5)` == `""`

### title_case
- `title_case("hello world")` == `"Hello World"`
- `title_case("hello-world")` == `"Hello World"`
- `title_case("hello_world")` == `"Hello World"`

### snake_case
- `snake_case("Hello World")` == `"hello_world"`
- `snake_case("helloWorld")` == `"hello_world"`
- `snake_case("kebab-case")` == `"kebab_case"`

### kebab_case
- `kebab_case("Hello World")` == `"hello-world"`
- `kebab_case("helloWorld")` == `"hello-world"`
- `kebab_case("snake_case")` == `"snake-case"`

### camel_case
- `camel_case("hello world")` == `"helloWorld"`
- `camel_case("Hello World")` == `"helloWorld"`
- `camel_case("snake_case")` == `"snakeCase"`

## Files
- `tests/__init__.py` (new, empty)
- `tests/support/__init__.py` (new, empty)
- `tests/support/test_str.py` (new)

## Reference Files
- `cara/support/Str.py` (function signatures and behavior)