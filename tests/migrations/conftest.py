"""Fixtures shared by the model-first migration tests."""

from __future__ import annotations

import pytest

from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer


@pytest.fixture
def discoverer() -> ModelDiscoverer:
    return ModelDiscoverer()
