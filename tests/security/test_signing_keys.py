from __future__ import annotations

import pytest

from cara.security import require_independent_signing_key, require_signing_keyring

_APP_KEY = "test-app-root-key-material-" * 2


def test_missing_key_is_rejected_in_every_environment() -> None:
    with pytest.raises(RuntimeError, match="at least 32 bytes"):
        require_independent_signing_key(
            value="",
            variable_name="QUEUE_SIGNING_KEY",
        )


def test_explicit_short_key_never_falls_back() -> None:
    with pytest.raises(RuntimeError, match="at least 32 bytes"):
        require_independent_signing_key(
            value="short",
            variable_name="QUEUE_SIGNING_KEY",
        )


def test_explicit_independent_key_is_returned() -> None:
    queue_key = "queue-signing-key-material-" * 2

    assert (
        require_independent_signing_key(
            value=queue_key,
            variable_name="QUEUE_SIGNING_KEY",
            disallowed={"APP_KEY": _APP_KEY},
        )
        == queue_key
    )


def test_reused_related_secret_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="independent from APP_KEY"):
        require_independent_signing_key(
            value=_APP_KEY,
            variable_name="QUEUE_SIGNING_KEY",
            disallowed={"APP_KEY": _APP_KEY},
        )


def test_queue_keyring_requires_explicit_active_id_and_previous_json() -> None:
    with pytest.raises(RuntimeError, match="QUEUE_SIGNING_KEY_ID"):
        require_signing_keyring(
            active_key_id="",
            active_key="active-queue-key-material-" * 2,
            previous_keys="{}",
        )
    with pytest.raises(RuntimeError, match="explicit JSON object"):
        require_signing_keyring(
            active_key_id="current",
            active_key="active-queue-key-material-" * 2,
            previous_keys=None,
        )


def test_queue_keyring_builds_rotation_map_and_rejects_duplicate_secrets() -> None:
    active = "active-queue-key-material-" * 2
    previous = "previous-queue-key-material-" * 2
    active_id, keys = require_signing_keyring(
        active_key_id="current",
        active_key=active,
        previous_keys={"previous": previous},
        disallowed={"APP_KEY": _APP_KEY},
    )
    assert active_id == "current"
    assert keys == {"current": active, "previous": previous}

    with pytest.raises(RuntimeError, match="independent|distinct"):
        require_signing_keyring(
            active_key_id="current",
            active_key=active,
            previous_keys={"previous": active},
        )
