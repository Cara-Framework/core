"""The dry run stages the build context BuildKit would send (DOCTRINE §2).

A deployable that keeps ``Dockerfile.dockerignore`` ships by it, not by its
``.dockerignore``; the patterns follow Docker's matching (``**/`` reaches the
root, ``*`` stays in one segment, ``!`` re-includes, the last rule wins).
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.VendorDryRun import _dockerignore, _ignored, _stage_tree


def _write(root: Path, rel: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")


def test_dockerfile_dockerignore_wins_over_dockerignore(tmp_path: Path) -> None:
    (tmp_path / ".dockerignore").write_text("app\n", encoding="utf-8")
    (tmp_path / "Dockerfile.dockerignore").write_text("tests\n", encoding="utf-8")
    rules = _dockerignore(tmp_path)
    assert _ignored("tests", "tests", rules)
    assert not _ignored("app", "app", rules)


def test_plain_dockerignore_is_the_fallback(tmp_path: Path) -> None:
    (tmp_path / ".dockerignore").write_text("docs\n", encoding="utf-8")
    assert _ignored("docs", "docs", _dockerignore(tmp_path))


def test_docker_pattern_semantics(tmp_path: Path) -> None:
    (tmp_path / ".dockerignore").write_text(
        "**/venv\n**/*.py[cod]\ndocs\n/storage/\n", encoding="utf-8"
    )
    rules = _dockerignore(tmp_path)
    assert _ignored("venv", "venv", rules)  # `**/` matches at the root too
    assert _ignored("a/b/venv", "venv", rules)
    assert _ignored("app/x.pyc", "x.pyc", rules)
    assert _ignored("storage", "storage", rules)
    assert not _ignored("app/docs", "docs", rules)  # a bare name is root-only
    assert not _ignored("app/x.py", "x.py", rules)


def test_negation_reincludes_and_the_last_rule_wins(tmp_path: Path) -> None:
    (tmp_path / ".dockerignore").write_text(
        "fixtures/*\n!fixtures/amazon\nfixtures/amazon/*\n!fixtures/amazon/ptd\n",
        encoding="utf-8",
    )
    for rel in (
        "fixtures/demo/a.json",
        "fixtures/amazon/raw/b.json",
        "fixtures/amazon/ptd/c.json",
    ):
        _write(tmp_path, rel)
    box = tmp_path.parent / f"{tmp_path.name}-box"
    _stage_tree(tmp_path, box, _dockerignore(tmp_path))
    assert (box / "fixtures/amazon/ptd/c.json").is_file()
    assert not (box / "fixtures/demo").exists()
    assert not (box / "fixtures/amazon/raw").exists()
