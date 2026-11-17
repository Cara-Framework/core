"""``local_database_refusal`` is a whitelist on both axes: environment and host."""

from __future__ import annotations

import pytest

from cara.configuration import (
    LOCAL_DATABASE_HOSTS,
    configured_database_host,
    configured_local_database_refusal,
    is_local_database_host,
    local_database_refusal,
)


@pytest.mark.parametrize("app_env", ["local", " Dev ", "DEVELOPMENT", "test", "testing"])
@pytest.mark.parametrize("db_host", sorted(LOCAL_DATABASE_HOSTS) + [" LocalHost "])
def test_local_env_and_local_host_pass(app_env: str, db_host: str) -> None:
    assert local_database_refusal(app_env=app_env, db_host=db_host) is None


@pytest.mark.parametrize(
    "app_env", ["production", "prod", "staging", "preprod", "ci", "", None]
)
def test_any_other_environment_is_refused(app_env: object) -> None:
    reason = local_database_refusal(app_env=app_env, db_host="127.0.0.1")
    assert reason is not None and "not a local environment" in reason


@pytest.mark.parametrize("db_host", ["db.internal", "10.0.0.5", "127.0.0.2", "", None])
def test_any_other_host_is_refused(db_host: object) -> None:
    assert not is_local_database_host(db_host)
    reason = local_database_refusal(app_env="local", db_host=db_host)
    assert reason is not None and "not a local database" in reason


def test_extra_environments_widen_the_environment_only() -> None:
    assert (
        local_database_refusal(
            app_env="CI", db_host="localhost", extra_environments=("ci",)
        )
        is None
    )
    assert "not a local database" in local_database_refusal(
        app_env="ci", db_host="db.internal", extra_environments=("ci",)
    )


def _reader(values: dict):
    return lambda key, default=None: values.get(key, default)


def test_configured_host_follows_the_default_connection() -> None:
    read = _reader(
        {
            "database.default": "replica",
            "database.drivers": {
                "app": {"host": "127.0.0.1"},
                "replica": {"host": "db.internal"},
            },
        }
    )
    assert configured_database_host(read) == "db.internal"


def test_configured_refusal_reads_env_and_host() -> None:
    local = _reader(
        {
            "app.env": "local",
            "database.default": "app",
            "database.drivers": {"app": {"host": "localhost"}},
        }
    )
    assert configured_local_database_refusal(local) is None


def test_an_unreadable_configuration_is_a_refusal() -> None:
    def broken(key, default=None):
        raise RuntimeError("no config")

    reason = configured_local_database_refusal(broken)
    assert reason is not None and "could not be verified" in reason


def test_environment_half_ignores_the_host() -> None:
    from cara.configuration import local_environment_refusal

    assert local_environment_refusal(app_env="dev") is None
    assert "staging" in local_environment_refusal(app_env="staging")
