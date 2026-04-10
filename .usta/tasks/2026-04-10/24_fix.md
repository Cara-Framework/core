# Fix: Add missing format_money bool test and pluralize edge cases

## Reviewer Summary
Tests were deleted instead of added

## Issues Found
- [error] tests/support/test_str.py:185 — The task is 'Add missing format_money bool test and pluralize edge cases' but the diff REMOVES test_format_money_rejects_bool and test_pluralize_edge_cases instead of adding them.

## Fix Instructions
In tests/support/test_str.py, restore the two deleted test functions after test_format_money_edge_cases (around line 183): (1) test_format_money_rejects_bool — imports pytest and format_money, then asserts pytest.raises(TypeError) for format_money(True) and format_money(False). (2) test_pluralize_edge_cases — imports pluralize, then asserts: pluralize('quiz')=='quizzes', pluralize('a')=='as', pluralize('bus')=='buses', pluralize('key')=='keys', pluralize('zoo')=='zoos', pluralize('fox')=='foxes'.