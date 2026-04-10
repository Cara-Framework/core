## Add missing edge-case tests for normalize_email and pluralize

The existing tests for `normalize_email` only cover basic stripping/lowering and empty strings. The `pluralize` tests miss z-ending words and single-character inputs. Add dedicated edge-case test functions.

### tests/support/test_str.py

Add these two new test functions (do not remove any existing tests):

```python
def test_normalize_email_edge_cases():
    from cara.support.Str import normalize_email
    # None input
    assert normalize_email(None) == ""
    # tabs and mixed whitespace
    assert normalize_email("\t Bob@Example.COM \n") == "bob@example.com"
    # already normalized
    assert normalize_email("alice@example.com") == "alice@example.com"


def test_pluralize_edge_cases():
    from cara.support.Str import pluralize
    # z-ending
    assert pluralize("quiz") == "quizzes"
    # single character
    assert pluralize("a") == "as"
    # s-ending
    assert pluralize("bus") == "buses"
    # vowel + y
    assert pluralize("key") == "keys"
    # vowel + o (should just add s)
    assert pluralize("zoo") == "zoos"
```
