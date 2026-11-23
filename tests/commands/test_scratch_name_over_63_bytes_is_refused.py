"""A scratch database name over 63 bytes is refused before any DDL.

Live defect (``cara/schema/Scratch.py`` ``validate_name``, called by
``schema:verify``): the scratch name was checked only for shape and for
equality with the configured database. PostgreSQL cuts an identifier to 63
bytes, so a scratch derived from a 63-byte configured name plus
``_verify`` was STORED as the configured database's name — and the very
next statements are ``DROP DATABASE IF EXISTS ... WITH (FORCE)``. Such a
name is now refused, and ``schema:verify`` refuses it with exit 2 before
running anything.
"""

from __future__ import annotations

import pytest

from cara.commands.core.SchemaVerifyCommand import SchemaVerifyCommand
from cara.exceptions import ScratchDatabaseException
from cara.schema import Scratch

CONFIGURED = "a" * 63


def test_a_derived_name_that_postgres_would_cut_is_refused() -> None:
    scratch = Scratch.derive_name(CONFIGURED, "verify")
    assert scratch.encode()[:63].decode() == CONFIGURED, "the hazard itself"

    with pytest.raises(ScratchDatabaseException, match="63 bytes"):
        Scratch.validate_name(scratch, CONFIGURED)


def test_exactly_63_bytes_is_accepted() -> None:
    Scratch.validate_name("b" * 63, CONFIGURED)


def test_an_ordinary_scratch_name_is_accepted() -> None:
    Scratch.validate_name("synkronus_verify", "synkronus")


class _Verify(SchemaVerifyCommand):
    def __init__(self, database: str) -> None:
        self.application = None
        self._parsed_options = {}
        self.messages: list[str] = []
        self.admin_batches: list[list[str]] = []
        self.craft_calls: list = []
        self._params = {"driver": "postgres", "database": database}

    def info(self, message: str) -> None:
        self.messages.append(message)

    def error(self, message: str, **_kwargs) -> None:
        self.messages.append(message)

    warning = success = info

    def _connection_params(self) -> dict:
        return self._params

    def _admin_sql(self, params, statements) -> None:
        self.admin_batches.append(list(statements))

    def _run_craft(self, arguments, scratch_database) -> int:
        self.craft_calls.append(arguments)
        return 0


def test_schema_verify_refuses_before_creating_or_dropping_anything() -> None:
    command = _Verify(CONFIGURED)

    assert command.handle() == 2
    assert command.admin_batches == []
    assert command.craft_calls == []
    assert any("63 bytes" in message for message in command.messages)
