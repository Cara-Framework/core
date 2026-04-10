import pytest
from cara.support.Str import (
    slugify,
    normalize_email,
    format_money,
    truncate,
    title_case,
    snake_case,
    kebab_case,
    camel_case,
)


def test_slugify():
    assert slugify("Hello World") == "hello-world"
    assert slugify("  ") == ""
    assert slugify("Héllo Wörld") == "hello-world"
    assert slugify("Hello World", "_") == "hello_world"


def test_normalize_email():
    assert normalize_email(" Alice@Example.COM ") == "alice@example.com"
    assert normalize_email("") == ""
    assert normalize_email("  ") == ""


def test_normalize_email_edge_cases():
    from cara.support.Str import normalize_email
    # None input
    assert normalize_email(None) == ""
    # tabs and mixed whitespace
    assert normalize_email("\t Bob@Example.COM \n") == "bob@example.com"
    # already normalized
    assert normalize_email("alice@example.com") == "alice@example.com"


def test_format_money():
    assert format_money(1050) == "$10.50"
    assert format_money(0) == "$0.00"
    assert format_money(100000, "EUR") == "€1,000.00"
    
    with pytest.raises(TypeError):
        format_money("1050")
    
    with pytest.raises(ValueError):
        format_money(-100)


def test_truncate():
    assert truncate("Hello World", 5) == "Hello..."
    assert truncate("Hi", 10) == "Hi"
    assert truncate("Hello", 3, "!") == "Hel!"
    assert truncate("", 5) == ""


def test_title_case():
    assert title_case("hello world") == "Hello World"
    assert title_case("hello-world") == "Hello World"
    assert title_case("hello_world") == "Hello World"


def test_title_case_edge_cases():
    assert title_case("  foo__bar--baz  ") == "Foo Bar Baz"
    assert title_case("___hello___") == "Hello"
    assert title_case("---") == ""
    assert title_case("   ") == ""
    assert title_case("hello_WORLD") == "Hello World"


def test_snake_case():
    assert snake_case("Hello World") == "hello_world"
    assert snake_case("helloWorld") == "hello_world"
    assert snake_case("kebab-case") == "kebab_case"


def test_kebab_case():
    assert kebab_case("Hello World") == "hello-world"
    assert kebab_case("helloWorld") == "hello-world"
    assert kebab_case("snake_case") == "snake-case"


def test_camel_case():
    assert camel_case("hello world") == "helloWorld"
    assert camel_case("Hello World") == "helloWorld"
    assert camel_case("snake_case") == "snakeCase"


def test_snake_case_edge_cases():
    from cara.support.Str import snake_case
    # acronyms: snake_case only splits lower-to-upper transitions, so
    # consecutive uppercase letters stay glued together
    assert snake_case("HTTPServer") == "httpserver"
    assert snake_case("getHTTPResponse") == "get_httpresponse"
    # consecutive non-alphanumeric characters collapse to a single underscore
    assert snake_case("foo--bar__baz") == "foo_bar_baz"
    assert snake_case("  foo  bar  ") == "foo_bar"
    # digit boundaries: digits are alphanumeric, so a letter/number run stays together
    assert snake_case("user123name") == "user123name"
    assert snake_case("item2Value") == "item2_value"
    # empty / None-ish input
    assert snake_case("") == ""
    assert snake_case(None) == ""


def test_kebab_case_edge_cases():
    from cara.support.Str import kebab_case
    assert kebab_case("HTTPServer") == "httpserver"
    assert kebab_case("getHTTPResponse") == "get-httpresponse"
    assert kebab_case("foo--bar__baz") == "foo-bar-baz"
    assert kebab_case("  foo  bar  ") == "foo-bar"
    assert kebab_case("item2Value") == "item2-value"
    assert kebab_case("") == ""
    assert kebab_case(None) == ""


def test_camel_case_edge_cases():
    from cara.support.Str import camel_case
    # underscores, hyphens, and spaces are all valid separators
    assert camel_case("hello_world") == "helloWorld"
    assert camel_case("hello-world") == "helloWorld"
    assert camel_case("hello world") == "helloWorld"
    # multiple separators and surrounding whitespace
    assert camel_case("  foo__bar--baz  ") == "fooBarBaz"
    # single-word input: first word is fully lowercased
    assert camel_case("Hello") == "hello"
    assert camel_case("HELLO") == "hello"
    # empty / None-ish input
    assert camel_case("") == ""
    assert camel_case(None) == ""


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


def test_studly_case():
    from cara.support.Str import studly_case
    assert studly_case("foo_bar") == "FooBar"
    assert studly_case("hello-world") == "HelloWorld"
    assert studly_case("  foo__bar--baz  ") == "FooBarBaz"
    assert studly_case("") == ""
    assert studly_case("   ") == ""
    assert studly_case("hello_WORLD") == "HelloWorld"


def test_studly_case_edge_cases():
    from cara.support.Str import studly_case
    # underscores, hyphens, and spaces are all valid separators
    assert studly_case("hello_world") == "HelloWorld"
    assert studly_case("hello-world") == "HelloWorld"
    assert studly_case("hello world") == "HelloWorld"
    # multiple separators and surrounding whitespace
    assert studly_case("  foo__bar--baz  ") == "FooBarBaz"
    # single-word input: first word is capitalized
    assert studly_case("Hello") == "Hello"
    assert studly_case("HELLO") == "Hello"
    # empty / None-ish input
    assert studly_case("") == ""
    assert studly_case(None) == ""


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


def test_slugify_unicode_extended():
    from cara.support.Str import slugify
    # Additional Unicode normalization tests
    assert slugify(u"na\xefve r\xe9sum\xe9") == "naive-resume"
    assert slugify(u"\u00e9cole") == "ecole"  # école
    assert slugify(u"\u00f1o\u00f1o") == "nono"  # ñoño


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
