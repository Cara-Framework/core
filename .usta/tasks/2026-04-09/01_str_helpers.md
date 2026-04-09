## Goal
Add five new string helper functions to `cara/support/Str.py`: `truncate`, `title_case`, `snake_case`, `kebab_case`, and `camel_case`.

## Steps
1. Open `cara/support/Str.py` and add the following functions **after** the existing `format_money` function (at the end of the file). Preserve all existing code exactly as-is.
2. Implement `truncate(text: str, limit: int, suffix: str = "...") -> str`:
   - If `text` length is <= `limit`, return `text` unchanged.
   - Otherwise return `text[:limit] + suffix`.
   - If `text` is empty or None, return `""`.
3. Implement `title_case(text: str) -> str`:
   - Split on whitespace, underscores, and hyphens.
   - Capitalize the first letter of each word, lowercase the rest.
   - Join with spaces.
   - Return `""` for empty/None input.
4. Implement `snake_case(text: str) -> str`:
   - Insert underscore before uppercase letters (camelCase → camel_Case).
   - Replace hyphens/spaces/consecutive non-alphanumeric chars with single underscore.
   - Lowercase everything, strip leading/trailing underscores.
   - Return `""` for empty/None input.
5. Implement `kebab_case(text: str) -> str`:
   - Same logic as `snake_case` but use hyphens instead of underscores.
   - Return `""` for empty/None input.
6. Implement `camel_case(text: str) -> str`:
   - Split on whitespace, underscores, and hyphens.
   - First word is fully lowercased; subsequent words have first letter capitalized, rest lowercased.
   - Join without separator.
   - Return `""` for empty/None input.
7. All functions must have type hints and docstrings matching the existing style in the file (see `slugify`, `normalize_email`, `format_money` for reference).

## Files
- `cara/support/Str.py`

## Reference Files
- `cara/support/__init__.py` (to see current exports — do NOT modify in this task)