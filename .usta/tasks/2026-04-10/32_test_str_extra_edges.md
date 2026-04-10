## Add edge-case tests for studly_case, slugify, and match

Expand coverage in existing test files with edge cases not yet covered.

### tests/support/test_str.py

Append these new test functions (do NOT remove or modify existing tests):

```python
def test_studly_case_edge_cases():
    from cara.support.Str import studly_case
    # single word
    assert studly_case("hello") == "Hello"
    # already studly
    assert studly_case("HelloWorld") == "Helloworld"
    # hyphens
    assert studly_case("foo-bar-baz") == "FooBarBaz"
    # mixed delimiters
    assert studly_case("foo_bar-baz") == "FooBarBaz"
    # None input
    assert studly_case(None) == ""


def test_slugify_unicode_extended():
    from cara.support.Str import slugify
    # German eszett
    assert slugify("Straße") == "strasse"
    # Spanish tilde
    assert slugify("señor") == "senor"
    # numbers preserved
    assert slugify("item 42 test") == "item-42-test"
    # all special chars
    assert slugify("!!!") == ""
    # custom separator with unicode
    assert slugify("café latte", "_") == "cafe_latte"
```

### tests/support/test_str_utils.py

Append this new test function (do NOT remove or modify existing tests):

```python
def test_match_double_wildcard():
    from cara.support.Str import match
    # middle wildcard with multiple segments
    assert match("app.http.controllers.UserController", "app.*.UserController")
    # leading wildcard
    assert match("deep.nested.module.Foo", "*.Foo")
    # trailing wildcard
    assert match("app.models.User", "app.*")
    # no match
    assert not match("app.controllers.Foo", "other.*")
    # exact match no wildcard
    assert match("exact.match", "exact.match")
    # no match exact
    assert not match("exact.match", "exact.mismatch")
```
