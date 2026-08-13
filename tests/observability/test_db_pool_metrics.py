"""Tests for the DB connection-pool saturation gauges in MetricsBase.

Covers:

* the three gauges are registered on the shared Prometheus registry with
  the namespaced names,
* :func:`sample_db_pool_metrics` is a safe no-op (does not raise, leaves
  gauges untouched) when the pool has never been initialised, and
* when the Postgres pool module reports live state, the sampler derives
  ``in_use`` / ``idle`` / ``max`` correctly and writes the gauges.

The pool's module-level state is patched directly (the same state the
real connection code mutates) rather than spinning up Postgres.
"""

from __future__ import annotations

import importlib
import threading

import pytest

from cara.observability.MetricsBase import (
    REGISTRY,
    MetricsBase,
    metric_name,
    sample_db_pool_metrics,
)

RuntimeMetrics = importlib.import_module("cara.observability._RuntimeMetrics")
_PG = importlib.import_module("cara.eloquent.connections.PostgresConnection")


@pytest.fixture
def restore_pool_state():
    """Snapshot + restore the Postgres pool module globals."""
    saved = (
        _PG._pool_initialized,
        _PG._pool_semaphore,
        _PG._pool_max_size,
        list(_PG.CONNECTION_POOL),
    )
    try:
        yield
    finally:
        _PG._pool_initialized = saved[0]
        _PG._pool_semaphore = saved[1]
        _PG._pool_max_size = saved[2]
        _PG.CONNECTION_POOL[:] = saved[3]


@pytest.fixture
def fake_pool_config():
    """Make the live pool publish a ceiling of 20."""
    _PG._pool_max_size = 20


# ── Registration ─────────────────────────────────────────────────────


def test_pool_gauges_registered_with_namespaced_names():
    names = REGISTRY._names_to_collectors
    assert metric_name("db_pool_connections_in_use") in names
    assert metric_name("db_pool_connections_idle") in names
    assert metric_name("db_pool_connections_max") in names


def test_pool_gauges_are_attributes_on_metrics_base():
    assert hasattr(MetricsBase, "db_pool_connections_in_use")
    assert hasattr(MetricsBase, "db_pool_connections_idle")
    assert hasattr(MetricsBase, "db_pool_connections_max")


# ── Uninitialised pool ───────────────────────────────────────────────


def test_read_stats_returns_none_when_pool_uninitialised(restore_pool_state):
    _PG._pool_initialized = False
    _PG._pool_semaphore = None
    assert RuntimeMetrics._read_db_pool_stats() is None


def test_sample_is_noop_and_does_not_raise_when_uninitialised(restore_pool_state):
    _PG._pool_initialized = False
    _PG._pool_semaphore = None
    # Must not raise.
    sample_db_pool_metrics()


def test_read_stats_returns_none_when_initialised_but_semaphore_missing(
    restore_pool_state,
):
    # Defensive: initialised flag set but semaphore somehow None.
    _PG._pool_initialized = True
    _PG._pool_semaphore = None
    assert RuntimeMetrics._read_db_pool_stats() is None


def test_read_stats_rejects_missing_pool_ceiling(restore_pool_state, monkeypatch):
    _PG._pool_initialized = True
    _PG._pool_semaphore = threading.Semaphore(1)
    _PG._pool_max_size = None

    with pytest.raises(RuntimeError, match="publish a positive"):
        RuntimeMetrics._read_db_pool_stats()


def test_read_stats_rejects_inconsistent_free_slot_count(
    restore_pool_state, fake_pool_config
):
    _PG._pool_initialized = True
    _PG._pool_semaphore = threading.Semaphore(21)

    with pytest.raises(RuntimeError, match="exceeds"):
        RuntimeMetrics._read_db_pool_stats()


# ── Live pool state ──────────────────────────────────────────────────


def test_read_stats_derives_in_use_idle_max(restore_pool_state, fake_pool_config):
    sem = threading.Semaphore(20)
    for _ in range(12):  # 12 checked out -> 8 free
        sem.acquire()
    _PG._pool_semaphore = sem
    _PG.CONNECTION_POOL = [object(), object()]  # 2 idle/warm
    _PG._pool_initialized = True

    stats = RuntimeMetrics._read_db_pool_stats()
    assert stats == {"in_use": 12, "idle": 2, "max": 20}


def test_sample_writes_gauge_values(restore_pool_state, fake_pool_config):
    sem = threading.Semaphore(20)
    for _ in range(5):  # 5 checked out -> 15 free
        sem.acquire()
    _PG._pool_semaphore = sem
    _PG.CONNECTION_POOL = [object()]  # 1 idle
    _PG._pool_initialized = True

    sample_db_pool_metrics()

    assert MetricsBase.db_pool_connections_in_use._value.get() == 5
    assert MetricsBase.db_pool_connections_idle._value.get() == 1
    assert MetricsBase.db_pool_connections_max._value.get() == 20


def test_render_invokes_pool_sampler_without_raising(restore_pool_state):
    """``render()`` calls the sampler before serialising; with an
    uninitialised pool it must still produce a payload."""
    _PG._pool_initialized = False
    _PG._pool_semaphore = None
    from cara.observability import render

    payload, content_type = render(service="test", role="test")
    assert isinstance(payload, bytes)
    assert "text/plain" in content_type
