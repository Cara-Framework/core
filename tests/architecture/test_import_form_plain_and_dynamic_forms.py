"""ImportForm reads every import FORM, not only ``from X import …``.

The live defect: the deep-import rule walked only ``ImportFrom`` statements,
so the same reach spelled ``import app.services.channels.ChannelService``
(with or without ``as X``) or ``importlib.import_module("app.services.…")``
with a literal path slipped it; and the kernel-via-app-barrels rule likewise
missed a literal ``import_module("commons.…")``.
"""

from __future__ import annotations

import pytest

from cara.architecture.scanners import ImportForm

from ._fixtures import make_manifest, write


@pytest.fixture
def manifest(tmp_path):
    manifest = make_manifest(tmp_path, layers=("controllers", "services"))
    write(
        tmp_path / "app" / "services" / "channels" / "__init__.py",
        '"""Domain."""\n\n__all__ = ["ChannelService"]\n',
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ChannelService.py",
        "class ChannelService:\n    pass\n",
    )
    return manifest


def _consumer(tmp_path, source: str) -> None:
    write(tmp_path / "app" / "controllers" / "Consumer.py", source)


def _deep(manifest) -> list[str]:
    return [f.message for f in ImportForm.scan(manifest) if "deep import" in f.message]


@pytest.mark.parametrize(
    "source",
    [
        "import app.services.channels.ChannelService\n",
        "import app.services.channels.ChannelService as Channels\n",
    ],
)
def test_a_module_level_plain_import_of_a_leaf_is_a_deep_import(
    tmp_path, manifest, source
):
    _consumer(tmp_path, source)

    assert _deep(manifest) == [
        "deep import `import app.services.channels.ChannelService` — use the "
        "app.services barrel"
    ]


def test_a_plain_import_of_the_domain_package_is_not_deep(tmp_path, manifest):
    _consumer(tmp_path, "import app.services.channels as channels\n")

    assert ImportForm.scan(manifest) == []


def test_a_literal_import_module_of_a_leaf_is_a_deep_import_anywhere(tmp_path, manifest):
    _consumer(
        tmp_path,
        "import importlib\n\n\n"
        "def load():\n"
        '    return importlib.import_module("app.services.channels.ChannelService")\n',
    )

    assert any("import_module(" in message for message in _deep(manifest))


def test_a_literal_import_module_of_the_barrel_is_not_deep(tmp_path, manifest):
    _consumer(
        tmp_path,
        "from importlib import import_module\n\n\n"
        "def load():\n"
        '    return import_module("app.services.channels")\n',
    )

    assert ImportForm.scan(manifest) == []


def test_a_literal_import_module_of_the_kernel_is_a_finding(tmp_path, manifest):
    _consumer(
        tmp_path,
        "import importlib\n\n\n"
        "def load():\n"
        '    return importlib.import_module("commons.gates")\n',
    )

    assert any("app.* barrels" in f.message for f in ImportForm.scan(manifest))


def test_a_literal_import_module_of_an_app_barrel_is_not_a_kernel_reach(
    tmp_path, manifest
):
    _consumer(
        tmp_path,
        "import importlib\n\n\n"
        "def load():\n"
        '    return importlib.import_module("app.gates")\n',
    )

    assert ImportForm.scan(manifest) == []
