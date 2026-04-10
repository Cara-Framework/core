## Bug Fix: format_money accepts bool inputs

In Python, `bool` is a subclass of `int`, so `isinstance(True, int)` returns `True`. This means `format_money(True)` silently returns `"$0.01"` and `format_money(False)` returns `"$0.00"` instead of raising `TypeError`.

Add a `bool` guard before the `int` check in `cara/support/Str.py`, and add test cases in `tests/support/test_str.py`.

### cara/support/Str.py

```python
def format_money(cents: int, currency: str = "USD") -> str:
    """Format an integer cent amount as a currency string.

    Raises TypeError if cents is not an int.
    Raises ValueError if cents is negative or currency is unsupported.

    Supported currencies: USD ($), EUR (\u20ac), GBP (\u00a3), TRY (\u20ba), AUD (A$), CAD (C$).
    Output: \"<symbol><whole>.<frac>\" with comma thousands separator.
    """
    if isinstance(cents, bool):
        raise TypeError("cents must be an integer")
    if not isinstance(cents, int):
        raise TypeError("cents must be an integer")
    if cents < 0:
        raise ValueError("cents must be non-negative")

    currency = currency.upper()
    symbols = {
        "USD": "$", "EUR": "\u20ac", "GBP": "\u00a3", "TRY": "\u20ba",
        "AUD": "A$", "CAD": "C$",
    }
    if currency not in symbols:
        raise ValueError(f"unsupported currency: {currency}")

    symbol = symbols[currency]
    whole = cents // 100
    frac = cents % 100
    return f"{symbol}{whole:,}.{frac:02d}"
```

### tests/support/test_str.py

Add to the existing `test_format_money_edge_cases` function:

```python
def test_format_money_rejects_bool():
    import pytest
    from cara.support.Str import format_money
    with pytest.raises(TypeError):
        format_money(True)
    with pytest.raises(TypeError):
        format_money(False)
```
