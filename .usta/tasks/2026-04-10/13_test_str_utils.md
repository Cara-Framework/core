## Goal
Create `tests/support/test_str_utils.py` covering `random_string`, `modularize`, `as_filepath`, `removeprefix`, `removesuffix`, `match`, and `add_query_params` which currently have zero test coverage.

## Steps
1. Create `tests/support/test_str_utils.py` with the following content.

```python
import string as string_mod
from cara.support.Str import (
    random_string,
    modularize,
    as_filepath,
    removeprefix,
    removesuffix,
    match,
    add_query_params,
)


# --- random_string ---

def test_random_string_default_length():
    result = random_string()
    assert len(result) == 4


def test_random_string_custom_length():
    assert len(random_string(10)) == 10
    assert len(random_string(0)) == 0


def test_random_string_charset():
    allowed = string_mod.ascii_uppercase + string_mod.digits
    result = random_string(100)
    for ch in result:
        assert ch in allowed


# --- modularize ---

def test_modularize_unix_path():
    assert modularize("app/controllers/Foo.py") == "app.controllers.Foo"


def test_modularize_windows_path():
    assert modularize("app\\controllers\\Foo.py") == "app.controllers.Foo"


def test_modularize_no_extension():
    assert modularize("app/controllers/Foo") == "app.controllers.Foo"


def test_modularize_custom_suffix():
    assert modularize("app/controllers/Foo.ts", ".ts") == "app.controllers.Foo"


# --- as_filepath ---

def test_as_filepath_dotted():
    assert as_filepath("app.controllers.Foo") == "app/controllers/Foo"


def test_as_filepath_single():
    assert as_filepath("single") == "single"


def test_as_filepath_empty():
    assert as_filepath("") == ""


# --- removeprefix ---

def test_removeprefix_match():
    assert removeprefix("HelloWorld", "Hello") == "World"


def test_removeprefix_no_match():
    assert removeprefix("HelloWorld", "Bye") == "HelloWorld"


def test_removeprefix_empty_prefix():
    assert removeprefix("HelloWorld", "") == "HelloWorld"


# --- removesuffix ---

def test_removesuffix_match():
    assert removesuffix("HelloWorld", "World") == "Hello"


def test_removesuffix_no_match():
    assert removesuffix("HelloWorld", "Bye") == "HelloWorld"


def test_removesuffix_empty_suffix():
    assert removesuffix("HelloWorld", "") == "HelloWorld"


# --- match ---

def test_match_trailing_wildcard():
    assert match("app.controllers.Foo", "app.*") is True


def test_match_leading_wildcard():
    assert match("app.controllers.Foo", "*.Foo") is True


def test_match_middle_wildcard():
    assert match("app.controllers.Foo", "app.*.Foo") is True


def test_match_exact():
    assert match("app.controllers.Foo", "app.controllers.Foo") is True


def test_match_no_match():
    assert match("app.controllers.Foo", "app.controllers.Bar") is False
    assert match("app.controllers.Foo", "other.*") is False


# --- add_query_params ---

def test_add_query_params_simple():
    assert add_query_params("/search", {"q": "hello"}) == "/search?q=hello"


def test_add_query_params_merge():
    result = add_query_params("/search?q=hello", {"page": "2"})
    assert "q=hello" in result
    assert "page=2" in result


def test_add_query_params_empty_dict():
    assert add_query_params("/search", {}) == "/search"


def test_add_query_params_full_url():
    result = add_query_params("https://example.com/path", {"key": "val"})
    assert result == "https://example.com/path?key=val"
```

## Files
- `tests/support/test_str_utils.py` (new)

## Reference Files
- `cara/support/Str.py`