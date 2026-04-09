# Fix title_case: drop empty tokens

## Files

- cara/support/Str.py
- tests/support/test_str.py

## Task

In `cara/support/Str.py`, the `title_case` function currently does:

    words = re.split(r"[\s_-]+", text)
    return " ".join(word.capitalize() for word in words)

Change it so empty tokens (from leading/trailing separators) are filtered first, and an all-separator input returns empty string. Use the exact same pattern already used by `camel_case` right below it.

In `tests/support/test_str.py`, add ONE new test function named `test_title_case_edge_cases` that asserts:

- title_case("  foo__bar--baz  ") equals "Foo Bar Baz"
- title_case("___hello___") equals "Hello"
- title_case("---") equals ""
- title_case("   ") equals ""
- title_case("hello_WORLD") equals "Hello World"

Confirm `title_case` is imported at the top of the test file; add it to the import line if missing.

## Rules

- Only one edit to `title_case` — no other function touched.
- No new imports beyond what's needed.
- The old multi-line `words = re.split...` and `return " ".join...` MUST be replaced, not duplicated.
