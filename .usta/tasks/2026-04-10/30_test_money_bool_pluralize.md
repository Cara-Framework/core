## Add two missing test gaps flagged in earlier rounds

Two specific gaps were flagged repeatedly: (1) no test verifies `format_money(True)` / `format_money(False)` raise `TypeError`, and (2) pluralize edge cases are missing.

Add both as new test functions to `tests/support/test_str.py`. The applier regenerates the whole file, so ALL existing tests must be preserved unchanged — only append the two new functions at the end.

### tests/support/test_str.py

Append these two new test functions after the existing `test_truncate_edge_cases` (do NOT remove or modify any existing code):

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
    # z-ending word adds 'es'
    assert pluralize("quiz") == "quizzes"
    # single character just adds 's'
    assert pluralize("a") == "as"
    # s-ending adds 'es'
    assert pluralize("bus") == "buses"
    # vowel + y just adds 's'
    assert pluralize("key") == "keys"
    # vowel + o just adds 's'
    assert pluralize("zoo") == "zoos"
    # x-ending adds 'es'
    assert pluralize("fox") == "foxes"
    # sh-ending adds 'es'
    assert pluralize("wish") == "wishes"
```
