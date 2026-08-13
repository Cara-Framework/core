"""Runtime level filtering uses the validated provider-owned snapshot."""

from __future__ import annotations

import pytest

from cara.exceptions import ConfigurationException
from cara.logging import Logger


@pytest.fixture
def logger(monkeypatch) -> Logger:
    monkeypatch.setattr(
        Logger,
        "_config",
        {"channels": {"console": {"LEVEL": "WARNING"}}},
    )
    return object.__new__(Logger)


def test_filter_reads_the_injected_logging_snapshot(logger: Logger) -> None:
    assert logger._should_log_level("INFO") is False
    assert logger._should_log_level("ERROR") is True


def test_filter_rejects_unknown_call_level(logger: Logger) -> None:
    with pytest.raises(ValueError, match="Unknown log level"):
        logger._should_log_level("verbose")


def test_filter_rejects_malformed_console_level(logger: Logger) -> None:
    Logger._config["channels"]["console"]["LEVEL"] = "verbose"

    with pytest.raises(ConfigurationException, match="LEVEL"):
        logger._should_log_level("INFO")
