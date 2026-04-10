## Add missing tests: format_money rejects bool, and pluralize edge cases

Two gaps flagged in earlier rounds: (1) `format_money(True)` / `format_money(False)` should raise `TypeError` but no test verifies this, and (2) pluralize edge cases (z-ending, single char, vowel+o) are missing.

Add both to the existing `tests/support/test_str.py`. Since the applier regenerates the whole file, include ALL existing tests unchanged plus the new ones.

### tests/support/test_str.py

Append these two new test functions at the end of the file (do NOT remove or modify any existing tests):

```python
def test_format_money_rejects_bool():
    import pytest
    from cara.support.Str import format_money
    with pytest.raises(TypeError):
        format_money(True)
    with pytest.raises(TypeError):
        format_money(False)


def test_pluralize_edge_cases():
    from cara.support.Str import pluralize
    # z-ending: adds 'es'
    assert pluralize("quiz") == "quizzes"
    # single character: just adds 's'
    assert pluralize("a") == "as"
    # s-ending
    assert pluralize("bus") == "buses"
    # vowel + y: just adds 's'
    assert pluralize("key") == "keys"
    # vowel + o: just adds 's'
    assert pluralize("zoo") == "zoos"
    # x-ending
    assert pluralize("fox") == "foxes"
```
