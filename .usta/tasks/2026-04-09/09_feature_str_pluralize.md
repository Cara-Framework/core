## Goal
Add a new rule-based English pluralization helper `pluralize(word)` to cara's `Str.py`, export it from `cara/support/__init__.py`, and add one test. Pure stdlib; no new top-level imports in `Str.py`.

## Steps
1. Open `cara/support/Str.py` and append this new function AT THE BOTTOM of the file (after the existing `camel_case` function). Do NOT modify any other function, import, or docstring in the file.

```python
def pluralize(word: str) -> str:
    """Pluralize an English word using simple rule-based heuristics.

    Rules (applied in order):
    - Empty/None -> empty string.
    - consonant + 'y' -> replace 'y' with 'ies' (baby -> babies).
    - vowel + 'y' -> add 's' (day -> days).
    - ends with 's', 'x', 'z', 'ch', 'sh' -> add 'es' (box -> boxes).
    - consonant + 'o' -> add 'es' (hero -> heroes).
    - otherwise -> add 's'.

    Intentionally dumb — does not handle irregular nouns (child, foot).
    Returns empty string for empty/None input.
    """
    if not word:
        return ""
    vowels = "aeiou"
    lower = word.lower()
    if len(lower) >= 2 and lower[-1] == "y" and lower[-2] not in vowels:
        return word[:-1] + "ies"
    if lower.endswith(("s", "x", "z")) or lower.endswith(("ch", "sh")):
        return word + "es"
    if len(lower) >= 2 and lower[-1] == "o" and lower[-2] not in vowels:
        return word + "es"
    return word + "s"
```

2. Open `cara/support/__init__.py`. Find the existing line:
   ```python
   from .Str import slugify, normalize_email, format_money, truncate, title_case, snake_case, kebab_case, camel_case
   ```
   Append `, pluralize` at the end so it becomes:
   ```python
   from .Str import slugify, normalize_email, format_money, truncate, title_case, snake_case, kebab_case, camel_case, pluralize
   ```
   Do NOT reorder any existing name, and do NOT touch any other import in the file.
   If there is an `__all__` list in this file that already lists `slugify` / `camel_case`, add the string `"pluralize"` to the end of that list as well. If there is no `__all__` list that mentions the Str helpers, do nothing to `__all__`.
3. Open `tests/support/test_str.py` and append ONE new pytest function at the bottom of the file:

```python
def test_pluralize():
    from cara.support.Str import pluralize
    assert pluralize("cat") == "cats"
    assert pluralize("box") == "boxes"
    assert pluralize("bush") == "bushes"
    assert pluralize("church") == "churches"
    assert pluralize("baby") == "babies"
    assert pluralize("day") == "days"
    assert pluralize("hero") == "heroes"
    assert pluralize("radio") == "radios"
    assert pluralize("") == ""
    assert pluralize(None) == ""
```

4. DO NOT add any top-level imports in `Str.py` (the helper uses only built-ins).
5. DO NOT modify any existing function body in `Str.py`, `__init__.py`, or `test_str.py`. Only append.

## Acceptance Criteria
- `cara/support/Str.py` contains exactly one new function `pluralize` placed after `camel_case`.
- `cara/support/__init__.py` imports `pluralize` from `.Str` on the same line as the other Str helpers.
- `tests/support/test_str.py` has one new function `test_pluralize` at the bottom.
- `from cara.support import pluralize; pluralize("box")` returns `"boxes"`.
- No existing lines are modified or removed in any of the three files.

## Files
- cara/support/Str.py
- cara/support/__init__.py
- tests/support/test_str.py
