"""The APP_ENV vocabulary decides security gates; every spelling must agree."""

import pytest

from cara.configuration import (
    INSECURE_SECRET_DEFAULTS,
    is_insecure_secret,
    is_local_environment,
    is_production_environment,
    is_test_environment,
    normalize_environment,
)


@pytest.mark.parametrize("raw", ["production", "prod", "Production", " PROD ", "prod\n"])
def test_environment_production_spellings_all_count(raw):
    assert is_production_environment(raw)
    assert not is_local_environment(raw)


@pytest.mark.parametrize("raw", ["staging", "stage", "ci", "", None, "preprod"])
def test_environment_non_production_is_not_production(raw):
    assert not is_production_environment(raw)


def test_environment_staging_is_neither_local_nor_production():
    # "not production" and "a developer's machine" are different questions.
    assert not is_local_environment("staging")
    assert not is_production_environment("staging")


@pytest.mark.parametrize(
    "raw", ["local", " Local", "dev", "DEVELOPMENT", "test", "testing"]
)
def test_environment_local_spellings(raw):
    assert is_local_environment(raw)


def test_environment_test_is_local_but_local_is_not_test():
    assert is_test_environment("Testing ")
    assert is_local_environment("testing")
    assert not is_test_environment("local")


def test_environment_normalize_handles_none_and_whitespace():
    assert normalize_environment(None) == ""
    assert normalize_environment("  PrOd ") == "prod"


@pytest.mark.parametrize("value", [None, 123, "", "changeme", "x" * 31])
def test_environment_insecure_secret(value):
    assert is_insecure_secret(value)


def test_environment_secure_secret_needs_32_chars_and_no_placeholder():
    assert not is_insecure_secret("x" * 32)
    assert all(is_insecure_secret(value) for value in INSECURE_SECRET_DEFAULTS)
