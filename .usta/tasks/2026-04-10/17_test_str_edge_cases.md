## Goal
Expand `tests/support/test_str.py` by appending new edge-case test functions for `slugify`, `format_money`, and `truncate`. Do NOT modify any existing test functions.

## Steps
1. Append three new test functions at the end of the existing `tests/support/test_str.py`. All existing code (lines 1-144) must remain byte-for-byte identical.

Append the following code after the last line of the file:

```python


def test_slugify_edge_cases():
    assert slugify("") == ""
    assert slugify(None) == ""
    assert slugify("hello") == "hello"
    assert slugify("HELLO WORLD") == "hello-world"
    assert slugify("--hello--world--") == "hello-world"
    assert slugify(u"caf\xe9 r\xe9sum\xe9") == "cafe-resume"
    assert slugify(u"\u0130stanbul \u015eehri") == "istanbul-sehri"
    assert slugify("foo   bar") == "foo-bar"
    assert slugify("hello world", ".") == "hello.world"
    assert slugify("a&b=c") == "a-b-c"


def test_format_money_edge_cases():
    assert format_money(1) == "$0.01"
    assert format_money(99) == "$0.99"
    assert format_money(100) == "$1.00"
    assert format_money(1234567, "GBP") == u"\u00a312,345.67"
    assert format_money(0, "TRY") == u"\u20ba0.00"
    assert format_money(50, "AUD") == "A$0.50"
    assert format_money(50, "CAD") == "C$0.50"
    with pytest.raises(ValueError):
        format_money(100, "JPY")
    with pytest.raises(TypeError):
        format_money(10.5)
    with pytest.raises(TypeError):
        format_money(None)


def test_truncate_edge_cases():
    assert truncate(None, 5) == ""
    assert truncate("Hello", 5) == "Hello"
    assert truncate("Hello", 0) == "..."
    assert truncate("Hello", 5, "") == "Hello"
    assert truncate("Hello World", 5, "") == "Hello"
```

## Files
- `tests/support/test_str.py`

## Reference Files
- `cara/support/Str.py`