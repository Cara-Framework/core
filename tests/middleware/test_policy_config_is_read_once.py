"""Per-request middleware policies read their config section once.

``HandleCors``, ``SecurityHeaders``, ``CompressResponses`` and
``EnforceBodySizeLimit`` are built (or ask) on every request, and each fetched
its settings one dot-path at a time: eighteen walks through the configuration
tree per request — 35 us, a quarter of the framework's own time on a small
request — to read keys that sit together in four dicts. Each now fetches its
dict once (7 us in total).

The values must be exactly what the key-by-key reads returned, and a reader
that only answers full paths (how the test suites stub ``config``) must keep
working.
"""

from __future__ import annotations

import importlib

import pytest

from cara.configuration import Configuration
from cara.http.BodyLimits import BodyLimits

cors_module = importlib.import_module("cara.middleware.http.Cors")
security_module = importlib.import_module("cara.middleware.http._SecurityHeaderPolicy")
compress_module = importlib.import_module("cara.middleware.http.CompressResponses")
limits_module = importlib.import_module("cara.http.BodyLimits")


@pytest.fixture
def configuration(monkeypatch) -> Configuration:
    monkeypatch.setattr(Configuration, "_instance", None)
    configuration = Configuration.empty()
    configuration.set(
        "cors.cors",
        {
            "paths": ["api/*", "hooks/*"],
            "allowed_origins": ["https://app.example.com"],
            "max_age": None,
            "supports_credentials": True,
        },
    )
    configuration.set(
        "security.security",
        {"headers": {"X-Frame-Options": None, "X-Extra": 1}, "hsts": None},
    )
    configuration.set("compression.compression", {"min_size": 2048, "level": 4})
    configuration.set("server.max_body_size", 4096)
    configuration.set("server.max_files", 3)
    return configuration


def _counting(reader):
    keys: list[str] = []

    def counted(key, default=None):
        keys.append(key)
        return reader(key, default)

    return counted, keys


def test_cors_policy_is_one_read_with_the_same_values(configuration, monkeypatch):
    counted, keys = _counting(configuration.get)
    monkeypatch.setattr(cors_module.configuration, "config", counted)

    policy = cors_module.load_cors_policy()

    assert keys == ["cors.cors"]  # was eight
    assert policy == {
        key: configuration.get(f"cors.cors.{key}", default)
        for key, default in cors_module.CORS_DEFAULTS.items()
    }
    # A key present with ``None`` stays ``None``; an absent one takes its default.
    assert policy["max_age"] is None
    assert policy["allowed_methods"] == ["*"]


def test_security_header_policy_is_one_read_with_the_same_values(
    configuration, monkeypatch
):
    counted, keys = _counting(configuration.get)
    monkeypatch.setattr(security_module.configuration, "config", counted)

    headers, hsts, preload = security_module._load_security_header_policy()

    assert keys == ["security.security"]  # was three
    assert "X-Frame-Options" not in headers
    assert headers["X-Extra"] == "1"
    assert hsts is None  # explicit opt-out, not the default
    assert preload is False


def test_compression_config_is_one_read_with_the_same_values(configuration, monkeypatch):
    counted, keys = _counting(configuration.get)
    monkeypatch.setattr(compress_module, "config", counted)

    loaded = compress_module.CompressResponses._load_config()

    assert keys == ["compression.compression"]  # was four
    assert loaded == (True, 2048, 4, compress_module._DEFAULT_COMPRESSIBLE_PREFIXES)


def test_body_limits_are_one_read_with_the_same_values(configuration, monkeypatch):
    counted, keys = _counting(configuration.get)
    monkeypatch.setattr(limits_module, "config", counted)

    limits = BodyLimits.configured()

    assert keys == ["server"]  # was three
    assert limits == BodyLimits(
        body_bytes=4096, file_bytes=BodyLimits.DEFAULT_FILE_BYTES, files=3
    )


def test_invalid_limit_in_the_section_is_still_refused(configuration):
    configuration.set("server.max_file_size", "10MB")

    with pytest.raises(ValueError, match=r"server\.max_file_size"):
        BodyLimits.configured()


def test_reader_that_only_answers_full_paths_is_asked_key_by_key(monkeypatch):
    answers = {
        "cors.cors.allowed_origins": ["https://only.example.com"],
        "server.max_body_size": 1024,
        "compression.compression.level": 9,
        "security.security.hsts": "max-age=60",
    }

    def flat(key, default=None):
        return answers.get(key, default)

    monkeypatch.setattr(cors_module.configuration, "config", flat)
    monkeypatch.setattr(compress_module, "config", flat)
    monkeypatch.setattr(limits_module, "config", flat)

    assert cors_module.load_cors_policy()["allowed_origins"] == [
        "https://only.example.com"
    ]
    assert BodyLimits.configured().body_bytes == 1024
    assert compress_module.CompressResponses._load_config()[2] == 9
    assert security_module._load_security_header_policy(flat)[1] == "max-age=60"
