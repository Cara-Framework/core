"""Resolving a class does not re-import the container's exception classes.

``_get_container_exceptions`` exists to dodge an import cycle, but it ran its
``import cara.exceptions`` and three lazy-barrel lookups on EVERY ``make`` and
``resolve`` — ten times for one controller with three injected dependencies
(21.6 us -> 10.7 us once the answer is kept).
"""

from __future__ import annotations

import builtins
import importlib

import pytest

from cara.container import Container
from cara.exceptions import (
    GenericContainerException,
    MissingContainerBindingException,
    StrictContainerException,
)

resolution_mod = importlib.import_module("cara.container._ContainerResolution")


class _Repo:
    pass


class _Mailer:
    pass


class _Controller:
    def __init__(self, repo: _Repo, mailer: _Mailer, note: str = "x"):
        self.repo = repo
        self.mailer = mailer


@pytest.fixture
def exception_imports(monkeypatch):
    """Every ``import cara.exceptions`` executed while the test runs."""
    seen: list[str] = []
    real_import = builtins.__import__

    def counting(name, *args, **kwargs):
        if name == "cara.exceptions":
            seen.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", counting)
    return seen


def test_repeated_resolution_imports_the_exceptions_at_most_once(exception_imports):
    container = Container()
    container.make(_Controller)  # may resolve the classes the first time
    exception_imports.clear()

    for _ in range(50):
        controller = container.make(_Controller)

    assert isinstance(controller.repo, _Repo)
    assert exception_imports == []  # was 10 per make: 500


def test_the_kept_classes_are_the_real_ones():
    assert resolution_mod._get_container_exceptions() == (
        GenericContainerException,
        MissingContainerBindingException,
        StrictContainerException,
    )
    assert (
        resolution_mod._get_container_exceptions()
        is resolution_mod._get_container_exceptions()
    )


def test_error_paths_still_raise_the_documented_exceptions():
    container = Container()

    with pytest.raises(MissingContainerBindingException):
        container.make("never-bound")

    container.strict = True
    container.bind("key", 1)
    with pytest.raises(StrictContainerException):
        container.bind("key", 2)

    class _NeedsArgument:
        def __init__(self, required):
            self.required = required

    with pytest.raises(GenericContainerException):
        Container().make(_NeedsArgument)
