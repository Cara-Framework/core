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
