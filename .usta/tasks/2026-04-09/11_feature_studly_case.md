# Add studly_case helper (PascalCase)

## Files

- cara/support/Str.py
- cara/support/__init__.py
- tests/support/test_str.py

## Task

In `cara/support/Str.py`, add a new function `studly_case(text: str) -> str` directly after the existing `camel_case` function and before `pluralize`.

`studly_case` must behave exactly like `camel_case` except the FIRST word is also capitalized. Use the same empty-token filtering (`[w for w in re.split(r"[\s_-]+", text) if w]`) and the same `word.capitalize()` per token. Return empty string for empty, None, or all-separator input.

Expected behavior:

- studly_case("foo_bar") -> "FooBar"
- studly_case("hello-world") -> "HelloWorld"
- studly_case("  foo__bar--baz  ") -> "FooBarBaz"
- studly_case("") -> ""
- studly_case("   ") -> ""
- studly_case("hello_WORLD") -> "HelloWorld"

In `cara/support/__init__.py`:

- Add `studly_case` to the import line that currently pulls `camel_case` and `pluralize` from `.Str`.
- Add `"studly_case"` to the `__all__` list near the other `*_case` entries.

In `tests/support/test_str.py`:

- Add `studly_case` to the import at the top of the file.
- Add a new test function `test_studly_case` that asserts the six cases listed above.

## Rules

- `camel_case` MUST NOT be modified.
- No alias (no `pascal_case`). Only `studly_case`.
- `studly_case` must appear exactly once in `Str.py`.
- `studly_case` must appear exactly once in the `.Str` import line of `__init__.py` and exactly once in `__all__`.
