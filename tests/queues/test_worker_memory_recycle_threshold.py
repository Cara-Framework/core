"""WORKER_MEMORY_LIMIT_MB is a knob, not a suggestion.

The old ``max(limit_mb, 2048)`` floor silently discarded every configured
value below 2 GiB. Production set 768 MiB against a 2 GiB container cap,
so the graceful-recycle check could never fire before the kernel OOM
SIGKILL — the exact un-graceful death the threshold exists to prevent.
"""

from __future__ import annotations

import importlib

qwc_module = importlib.import_module("cara.commands.core.QueueWorkCommand")
QueueWorkCommand = qwc_module.QueueWorkCommand


def test_a_configured_value_below_two_gigabytes_is_respected(monkeypatch):
    monkeypatch.setattr(
        qwc_module, "config", lambda key, default=None: 768 if "memory" in key else default
    )

    assert QueueWorkCommand._resolve_memory_limit_mb() == 768


def test_the_default_without_configuration_is_two_gigabytes(monkeypatch):
    monkeypatch.setattr(qwc_module, "config", lambda key, default=None: default)

    assert QueueWorkCommand._resolve_memory_limit_mb() == 2048


def test_a_broken_configuration_falls_back_to_the_default(monkeypatch):
    def _boom(key, default=None):
        raise RuntimeError("config store unavailable")

    monkeypatch.setattr(qwc_module, "config", _boom)

    assert QueueWorkCommand._resolve_memory_limit_mb() == 2048
