from __future__ import annotations

import os
import stat
import threading

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


def test_a_failed_overwrite_leaves_the_stored_object_untouched(tmp_path) -> None:
    # ``open(key, "wb")`` truncated the key before the write could fail.
    driver = FileDriver(str(tmp_path / "store"))
    driver.put("photos/1.jpg", b"ORIGINAL")

    with pytest.raises(StorageException):
        driver.put("photos/1.jpg", "not bytes")  # type: ignore[arg-type]

    assert driver.get("photos/1.jpg") == b"ORIGINAL"
    assert [p.name for p in (tmp_path / "store" / "photos").iterdir()] == ["1.jpg"]


def test_a_reader_never_sees_a_half_written_overwrite(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))
    payloads = (b"a" * 4_000_000, b"b" * 4_000_000)
    driver.put("photos/1.jpg", payloads[0])
    seen: list[bytes] = []
    done = threading.Event()

    def read() -> None:
        while not done.is_set():
            seen.append(driver.get("photos/1.jpg"))

    reader = threading.Thread(target=read)
    reader.start()
    try:
        for turn in range(40):
            driver.put("photos/1.jpg", payloads[turn % 2])
    finally:
        done.set()
        reader.join()

    assert seen
    assert all(body in payloads for body in seen)


def test_put_survives_its_directory_being_pruned_by_a_concurrent_delete(
    tmp_path, monkeypatch
) -> None:
    driver = FileDriver(str(tmp_path / "store"))
    real_open = os.open
    pruned = []

    def open_after_a_prune(path, flags, mode=0o777, **kwargs):
        if not pruned:
            # What ``delete`` of the last sibling does between put's
            # ``makedirs`` and its create.
            pruned.append(path)
            os.rmdir(os.path.dirname(path))
        return real_open(path, flags, mode, **kwargs)

    monkeypatch.setattr(os, "open", open_after_a_prune)

    driver.put("imports/7/new.csv", b"payload")

    assert pruned
    assert driver.get("imports/7/new.csv") == b"payload"


def test_put_keeps_default_and_existing_permissions(tmp_path) -> None:
    driver = FileDriver(str(tmp_path / "store"))
    umask = os.umask(0)
    os.umask(umask)

    driver.put("public/a.txt", b"one")
    stored = tmp_path / "store" / "public" / "a.txt"
    assert stat.S_IMODE(stored.stat().st_mode) == 0o666 & ~umask

    stored.chmod(0o640)
    driver.put("public/a.txt", b"two")
    assert stat.S_IMODE(stored.stat().st_mode) == 0o640
    assert stored.read_bytes() == b"two"
