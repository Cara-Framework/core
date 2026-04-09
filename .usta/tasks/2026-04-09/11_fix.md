# Fix: Add studly_case helper (PascalCase)

1. In cara/support/__init__.py, add `studly_case` to the import line from .Str (alongside camel_case, pluralize, etc.).
2. In cara/support/__init__.py, add `"studly_case"` to the __all__ list in the String utilities section.
3. In tests/support/test_str.py, add a test function `test_studly_case` that imports and tests studly_case with cases like: studly_case('hello_world') == 'HelloWorld', studly_case('  foo__bar  ') == 'FooBar', studly_case('') == '', studly_case(None) == '', studly_case('already') == 'Already'.