from cara.support import currency_minor_digits, format_money


def test_zero_decimal_currencies_have_no_minor_unit():
    for code in ("JPY", "krw", " UYI ", "XOF"):
        assert currency_minor_digits(code) == 0


def test_other_and_unreadable_codes_step_in_cents():
    for code in ("USD", "EUR", None, "", "12$", "DOLLAR"):
        assert currency_minor_digits(code) == 2


def test_format_money_uses_the_same_exponent():
    assert format_money(1500, "JPY") == "¥1,500"
    assert format_money(1500, "UYI") == "UYI 1,500"
    assert format_money(12.5, "USD") == "$12.50"
