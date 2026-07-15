from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from cara.commands.core import ServeCommand


def _runtime_tree(root: Path) -> Path:
    for name in (
        "app",
        "config",
        "database",
        "routes",
        "storage",
        "tests",
        "venv",
    ):
        (root / name).mkdir(parents=True)

    shared = root.parent / "shared"
    (shared / "jobs").mkdir(parents=True)
    (shared / "models").mkdir(parents=True)
    (shared / "support").mkdir(parents=True)
    (shared / "cara" / "cara").mkdir(parents=True)
    (shared / "cara" / "tests").mkdir(parents=True)
    (shared / "cara" / ".venv").mkdir(parents=True)
    (root / "commons").symlink_to(shared, target_is_directory=True)
    (root / "cara").symlink_to(shared / "cara" / "cara", target_is_directory=True)
    return shared


def test_reload_scope_uses_event_driven_root_and_resolved_shared_source(tmp_path):
    project = tmp_path / "api"
    project.mkdir()
    shared = _runtime_tree(project)

    with patch(
        "cara.commands.core.ServeCommand.importlib.util.find_spec",
        return_value=object(),
    ):
        directories = ServeCommand._reload_directories(str(project))

    assert directories == [str(project.resolve()), str(shared.resolve())]

    excluded = ServeCommand._reload_excluded_directories(directories)
    assert str((project / "venv").resolve()) in excluded
    assert str((project / "tests").resolve()) in excluded
    assert str((project / "database").resolve()) in excluded
    assert str((shared / "cara" / "tests").resolve()) in excluded
    assert str((shared / "cara" / ".venv").resolve()) in excluded


def test_reload_scope_falls_back_to_runtime_packages_without_watchfiles(tmp_path):
    project = tmp_path / "api"
    project.mkdir()
    shared = _runtime_tree(project)

    with patch(
        "cara.commands.core.ServeCommand.importlib.util.find_spec",
        return_value=None,
    ):
        directories = ServeCommand._reload_directories(str(project))

    assert set(directories) == {
        str((project / "app").resolve()),
        str((project / "config").resolve()),
        str((project / "routes").resolve()),
        str((shared / "jobs").resolve()),
        str((shared / "models").resolve()),
        str((shared / "support").resolve()),
        str((shared / "cara" / "cara").resolve()),
    }
    assert str(shared.resolve()) not in directories
    assert str(project.resolve()) not in directories
    assert str((project / "venv").resolve()) not in directories
    assert str((project / "tests").resolve()) not in directories


def test_server_command_passes_reload_scope_to_uvicorn():
    command = ServeCommand.__new__(ServeCommand)
    config = {
        "host": "127.0.0.1",
        "port": 8400,
        "reload": True,
        "workers": 1,
    }

    with (
        patch.object(command, "_reload_directories", return_value=["/project/app"]),
        patch.object(
            command,
            "_reload_excluded_directories",
            return_value=["/project/tests", "/project/venv"],
        ),
        patch("cara.commands.core.ServeCommand.os.path.exists", return_value=False),
    ):
        built = command._build_server_command(config)

    assert built.count("--reload") == 1
    assert built[built.index("--reload-dir") + 1] == "/project/app"
    assert built.count("--reload-exclude") == 2
    assert "/project/tests" in built
    assert "/project/venv" in built
