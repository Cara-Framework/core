## Goal
Add edge-case unit tests for cara's `snake_case`, `kebab_case`, and `camel_case` helpers. The existing tests in `tests/support/test_str.py` cover the happy path but miss acronyms, consecutive separators, digits, and empty input. This task ONLY adds new tests — it does not modify any existing code.

## Steps
1. Open `tests/support/test_str.py`. It already contains tests for `slugify`, `normalize_email`, `format_money`, `truncate`, `title_case`, `snake_case`, `kebab_case`, `camel_case`. Leave every existing function untouched.
2. Append these three new pytest functions AT THE BOTTOM of the file (after the last existing test), in this exact order:

```python
def test_snake_case_edge_cases():
    from cara.support.Str import snake_case
    # acronyms: snake_case only splits lower-to-upper transitions, so
    # consecutive uppercase letters stay glued together
    assert snake_case("HTTPServer") == "httpserver"
    assert snake_case("getHTTPResponse") == "get_httpresponse"
    # consecutive non-alphanumeric characters collapse to a single underscore
    assert snake_case("foo--bar__baz") == "foo_bar_baz"
    assert snake_case("  foo  bar  ") == "foo_bar"
    # digit boundaries: digits are alphanumeric, so a letter/number run stays together
    assert snake_case("user123name") == "user123name"
    assert snake_case("item2Value") == "item2_value"
    # empty / None-ish input
    assert snake_case("") == ""
    assert snake_case(None) == ""


def test_kebab_case_edge_cases():
    from cara.support.Str import kebab_case
    assert kebab_case("HTTPServer") == "httpserver"
    assert kebab_case("getHTTPResponse") == "get-httpresponse"
    assert kebab_case("foo--bar__baz") == "foo-bar-baz"
    assert kebab_case("  foo  bar  ") == "foo-bar"
    assert kebab_case("item2Value") == "item2-value"
    assert kebab_case("") == ""
    assert kebab_case(None) == ""


def test_camel_case_edge_cases():
    from cara.support.Str import camel_case
    # underscores, hyphens, and spaces are all valid separators
    assert camel_case("hello_world") == "helloWorld"
    assert camel_case("hello-world") == "helloWorld"
    assert camel_case("hello world") == "helloWorld"
    # multiple separators and surrounding whitespace
    assert camel_case("  foo__bar--baz  ") == "fooBarBaz"
    # single-word input: first word is fully lowercased
    assert camel_case("Hello") == "hello"
    assert camel_case("HELLO") == "hello"
    # empty / None-ish input
    assert camel_case("") == ""
    assert camel_case(None) == ""
```

3. DO NOT touch any import, fixture, or existing `def test_*` function. Only append the three new functions above at the end of the file.
4. Each new test imports its target locally (inside the function body) so the top-of-file imports do not need to be touched.

## Acceptance Criteria
- `tests/support/test_str.py` has three new functions at the bottom named EXACTLY `test_snake_case_edge_cases`, `test_kebab_case_edge_cases`, `test_camel_case_edge_cases`.
- No existing line in the file is modified, reordered, or removed.
- The file parses as valid Python.
- All three new tests import their helper locally from `cara.support.Str`.

## Files
- tests/support/test_str.py

## Reference Files
- cara/support/Str.py
