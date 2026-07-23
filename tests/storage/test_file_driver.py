from __future__ import annotations

import pytest

from cara.exceptions import StorageException
from cara.storage.drivers import FileDriver


def test_keys_keep_their_directory_structure_and_extension(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))

    driver.put("imports/7/IMP123.csv", b"sku,title\nA,Alpha\n")

    stored = tmp_path / "store" / "imports" / "7" / "IMP123.csv"
    assert stored.read_bytes() == b"sku,title\nA,Alpha\n"
    assert driver.get("imports/7/IMP123.csv") == stored.read_bytes()
    assert not list((tmp_path / "store").rglob("*.bin"))


def test_hierarchical_keys_do_not_collapse_into_filename_collisions(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))

    driver.put("imports/a_b.csv", b"flat")
    driver.put("imports/a/b.csv", b"nested")

    assert driver.get("imports/a_b.csv") == b"flat"
    assert driver.get("imports/a/b.csv") == b"nested"


@pytest.mark.parametrize(
    "key",
    ("", ".", "../outside.csv", "imports/../../outside.csv", r"..\outside.csv"),
)
def test_unsafe_keys_cannot_escape_the_storage_root(tmp_path, key: str) -> None:
    driver = FileDriver(str(tmp_path / "store"))

    with pytest.raises(StorageException):
        driver.put(key, b"nope")

    assert not (tmp_path / "outside.csv").exists()


def test_symlinked_directory_cannot_escape_the_storage_root(tmp_path) -> None:
    root = tmp_path / "store"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "linked").symlink_to(outside, target_is_directory=True)
    driver = FileDriver(str(root))

    with pytest.raises(StorageException):
        driver.put("linked/escape.csv", b"nope")

    assert not (outside / "escape.csv").exists()


def test_delete_prunes_empty_key_directories(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))
    driver.put("imports/7/IMP123.csv", b"payload")

    assert driver.delete("imports/7/IMP123.csv") is True
    assert driver.delete("imports/7/IMP123.csv") is False
    assert not (tmp_path / "store" / "imports").exists()


def test_delete_directory_is_scoped_below_the_storage_root(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))
    driver.put("imports/7/a.csv", b"a")
    driver.put("exports/7/b.csv", b"b")

    assert driver.delete_directory("imports") is True
    assert driver.delete_directory("imports") is False
    assert driver.get("exports/7/b.csv") == b"b"

    with pytest.raises(StorageException):
        driver.delete_directory(".")
