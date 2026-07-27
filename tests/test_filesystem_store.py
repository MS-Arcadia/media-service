"""The filesystem object store, against a real directory.

The in-memory fake proves the use cases; this proves the adapter. They are different risks:
the fake cannot get atomicity, sharding or streaming wrong.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest

from app.adapters.outbound.filesystem import CHUNK_SIZE, FilesystemObjectStore
from app.domain.media import object_key_for
from app.platform import errors


@pytest.fixture
def store(tmp_path: Path) -> FilesystemObjectStore:
    return FilesystemObjectStore(tmp_path / "media")


async def test_the_root_is_created_if_it_does_not_exist(tmp_path: Path):
    root = tmp_path / "deep" / "nested" / "media"
    FilesystemObjectStore(root)
    assert root.is_dir()


async def test_bytes_survive_a_round_trip(store: FilesystemObjectStore):
    key = object_key_for("abcd1234")
    data = b"hello arcadia"

    checksum = await store.put(key, data)

    assert checksum == sha256(data).hexdigest()
    assert b"".join([chunk async for chunk in store.open(key)]) == data


async def test_a_large_object_streams_in_chunks(store: FilesystemObjectStore):
    """A 4 GB build read into memory would take the process down, once per download."""
    data = b"x" * (CHUNK_SIZE * 2 + 17)
    key = object_key_for("bigfile1")
    await store.put(key, data)

    chunks = [chunk async for chunk in store.open(key)]

    assert len(chunks) == 3
    assert [len(c) for c in chunks] == [CHUNK_SIZE, CHUNK_SIZE, 17]
    assert b"".join(chunks) == data


async def test_the_key_is_sharded_into_directories(store: FilesystemObjectStore, tmp_path: Path):
    """One flat directory with a million files is slow to list on most filesystems."""
    await store.put(object_key_for("abcd1234"), b"x")
    assert (tmp_path / "media" / "ab" / "cd" / "abcd1234").is_file()


async def test_no_partial_file_is_left_behind_after_a_successful_write(
    store: FilesystemObjectStore, tmp_path: Path
):
    await store.put(object_key_for("abcd1234"), b"x")
    assert list((tmp_path / "media" / ".tmp").glob("*.part")) == []


async def test_a_leftover_partial_write_is_swept(store: FilesystemObjectStore, tmp_path: Path):
    """A crash between opening the temporary file and the rename leaves one behind. Nothing
    references it, so without a sweep it is waste that only grows."""
    partial = tmp_path / "media" / ".tmp" / "orphan.part"
    partial.write_bytes(b"half a file")

    assert await store.sweep_temporary_files() == 1
    assert not partial.exists()


async def test_a_stored_object_is_not_swept(store: FilesystemObjectStore):
    key = object_key_for("abcd1234")
    await store.put(key, b"keep me")
    await store.sweep_temporary_files()
    assert await store.exists(key) is True


async def test_overwriting_replaces_the_contents(store: FilesystemObjectStore):
    key = object_key_for("abcd1234")
    await store.put(key, b"first")
    await store.put(key, b"second")
    assert b"".join([chunk async for chunk in store.open(key)]) == b"second"


async def test_deleting_removes_the_file_and_prunes_empty_shards(
    store: FilesystemObjectStore, tmp_path: Path
):
    key = object_key_for("abcd1234")
    await store.put(key, b"x")

    assert await store.delete(key) is True
    assert await store.exists(key) is False
    # Left alone, the shard directories accumulate until a store with a long history is
    # mostly empty directories.
    assert not (tmp_path / "media" / "ab" / "cd").exists()


async def test_deleting_something_absent_reports_that_nothing_happened(
    store: FilesystemObjectStore,
):
    assert await store.delete(object_key_for("nothere1")) is False


async def test_a_shard_holding_another_object_is_not_pruned(
    store: FilesystemObjectStore, tmp_path: Path
):
    await store.put("ab/cd/first", b"1")
    await store.put("ab/cd/second", b"2")

    await store.delete("ab/cd/first")

    assert (tmp_path / "media" / "ab" / "cd").is_dir()
    assert await store.exists("ab/cd/second") is True


async def test_usage_excludes_temporary_files(store: FilesystemObjectStore, tmp_path: Path):
    await store.put(object_key_for("abcd1234"), b"x" * 100)
    (tmp_path / "media" / ".tmp" / "junk.part").write_bytes(b"y" * 500)

    assert await store.usage_bytes() == 100


async def test_a_key_escaping_the_root_is_refused(store: FilesystemObjectStore):
    """Cannot happen with keys generated from a media id, which is why it raises INTERNAL
    rather than a client error: reaching it means a bug in a caller, not a bad request."""
    with pytest.raises(errors.AppError) as caught:
        await store.put("../../escaped", b"x")
    assert caught.value.reason == "INVALID_OBJECT_KEY"


async def test_an_absolute_key_cannot_escape_either(store: FilesystemObjectStore):
    with pytest.raises(errors.AppError):
        await store.put("/etc/passwd", b"x")
