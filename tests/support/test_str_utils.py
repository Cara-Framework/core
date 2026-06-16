from cara.support.Str import (
    as_filepath,
    modularize,
    removesuffix,
)

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


def test_as_filepath_single_segment():
    assert as_filepath("single") == "single"


def test_as_filepath_empty():
    assert as_filepath("") == ""


# --- removesuffix ---


def test_removesuffix_match():
    assert removesuffix("HelloWorld", "World") == "Hello"


def test_removesuffix_no_match():
    assert removesuffix("HelloWorld", "Bye") == "HelloWorld"


def test_removesuffix_empty_suffix():
    assert removesuffix("HelloWorld", "") == "HelloWorld"
