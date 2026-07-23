from __future__ import annotations

from cara.environment import LoadEnvironment


def test_environment_files_are_resolved_from_the_application_base(
    tmp_path,
    monkeypatch,
) -> None:
    application_root = tmp_path / "application"
    caller_root = tmp_path / "caller"
    application_root.mkdir()
    caller_root.mkdir()
    (application_root / ".env").write_text(
        "CARA_BASE_PATH_TEST=from-application\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(caller_root)
    monkeypatch.delenv("CARA_BASE_PATH_TEST", raising=False)

    LoadEnvironment(base_path=application_root)

    import os

    assert os.environ["CARA_BASE_PATH_TEST"] == "from-application"
