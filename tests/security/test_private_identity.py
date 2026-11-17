"""One rule for "this carries a person's identity" on append-only ledgers."""

from __future__ import annotations

from cara.security import (
    REDACTED,
    contains_private_identity,
    redact_private_identity,
)


def test_values_keys_and_identity_key_names_are_all_inspected() -> None:
    assert contains_private_identity({"note": "bob@example.com"})
    assert contains_private_identity({"bob@example.com": True})
    assert contains_private_identity({"ip_address": None})
    assert contains_private_identity(["ok", {"x": "203.0.113.9"}])
    assert contains_private_identity({"note": "write to bob@example.com today"})
    assert not contains_private_identity({"name": "Acme", "count": 3, "v": "1.2"})


def test_redaction_keeps_the_record_but_not_the_identity() -> None:
    redacted = redact_private_identity(
        {
            "email": "bob@example.com",
            "Email": None,
            "note": "write to bob@example.com today",
            "alice@example.com": {"role": "admin"},
            "peers": ["10.0.0.1", 3],
            "name": "Acme",
        }
    )
    assert redacted == {
        "email": REDACTED,
        "Email": None,
        "note": f"write to {REDACTED} today",
        REDACTED: {"role": "admin"},
        "peers": [REDACTED, 3],
        "name": "Acme",
    }
    assert not contains_private_identity(
        {k: v for k, v in redacted.items() if k not in ("email", "Email")}
    )


def test_too_deep_fails_closed() -> None:
    deep: dict = {}
    node = deep
    for _ in range(20):
        node["x"] = {}
        node = node["x"]
    assert contains_private_identity(deep)
