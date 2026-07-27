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


GENEROUS = 1024 * 1024 * 1024


async def one(data: bytes):
    yield data


async def chunked(data: bytes, size: int):
    for start in range(0, len(data), size):
        yield data[start : start + size]


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

    checksum, size = await store.put(key, one(data), max_bytes=GENEROUS)

    assert checksum == sha256(data).hexdigest()
    assert b"".join([chunk async for chunk in store.open(key)]) == data


async def test_a_large_object_streams_in_chunks(store: FilesystemObjectStore):
    """A 4 GB build read into memory would take the process down, once per download."""
    data = b"x" * (CHUNK_SIZE * 2 + 17)
    key = object_key_for("bigfile1")
    await store.put(key, one(data), max_bytes=GENEROUS)

    chunks = [chunk async for chunk in store.open(key)]

    assert len(chunks) == 3
    assert [len(c) for c in chunks] == [CHUNK_SIZE, CHUNK_SIZE, 17]
    assert b"".join(chunks) == data


async def test_the_key_is_sharded_into_directories(store: FilesystemObjectStore, tmp_path: Path):
    """One flat directory with a million files is slow to list on most filesystems."""
    await store.put(object_key_for("abcd1234"), one(b"x"), max_bytes=GENEROUS)
    assert (tmp_path / "media" / "ab" / "cd" / "abcd1234").is_file()


async def test_no_partial_file_is_left_behind_after_a_successful_write(
    store: FilesystemObjectStore, tmp_path: Path
):
    await store.put(object_key_for("abcd1234"), one(b"x"), max_bytes=GENEROUS)
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
    await store.put(key, one(b"keep me"), max_bytes=GENEROUS)
    await store.sweep_temporary_files()
    assert await store.exists(key) is True


async def test_overwriting_replaces_the_contents(store: FilesystemObjectStore):
    key = object_key_for("abcd1234")
    await store.put(key, one(b"first"), max_bytes=GENEROUS)
    await store.put(key, one(b"second"), max_bytes=GENEROUS)
    assert b"".join([chunk async for chunk in store.open(key)]) == b"second"


async def test_deleting_removes_the_file_and_prunes_empty_shards(
    store: FilesystemObjectStore, tmp_path: Path
):
    key = object_key_for("abcd1234")
    await store.put(key, one(b"x"), max_bytes=GENEROUS)

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
    await store.put("ab/cd/first", one(b"1"), max_bytes=GENEROUS)
    await store.put("ab/cd/second", one(b"2"), max_bytes=GENEROUS)

    await store.delete("ab/cd/first")

    assert (tmp_path / "media" / "ab" / "cd").is_dir()
    assert await store.exists("ab/cd/second") is True


async def test_usage_excludes_temporary_files(store: FilesystemObjectStore, tmp_path: Path):
    await store.put(object_key_for("abcd1234"), one(b"x" * 100), max_bytes=GENEROUS)
    (tmp_path / "media" / ".tmp" / "junk.part").write_bytes(b"y" * 500)

    assert await store.usage_bytes() == 100


async def test_a_key_escaping_the_root_is_refused(store: FilesystemObjectStore):
    """Cannot happen with keys generated from a media id, which is why it raises INTERNAL
    rather than a client error: reaching it means a bug in a caller, not a bad request."""
    with pytest.raises(errors.AppError) as caught:
        await store.put("../../escaped", one(b"x"), max_bytes=GENEROUS)
    assert caught.value.reason == "INVALID_OBJECT_KEY"


async def test_an_absolute_key_cannot_escape_either(store: FilesystemObjectStore):
    with pytest.raises(errors.AppError):
        await store.put("/etc/passwd", one(b"x"), max_bytes=GENEROUS)


# --- streaming ----------------------------------------------------------


async def test_a_multi_chunk_stream_is_reassembled_exactly(store: FilesystemObjectStore):
    data = bytes(range(256)) * 5000
    key = object_key_for("streamed")

    checksum, size = await store.put(key, chunked(data, 4096), max_bytes=GENEROUS)

    assert size == len(data)
    assert checksum == sha256(data).hexdigest()
    assert b"".join([chunk async for chunk in store.open(key)]) == data


async def test_an_oversized_stream_is_cut_off_mid_write(store: FilesystemObjectStore):
    """The limit is enforced while writing, not after.

    Waiting until the end would mean putting the whole oversized file on disk before rejecting
    it — exactly the resource exhaustion the limit exists to prevent.
    """
    data = b"x" * 10_000
    with pytest.raises(errors.AppError) as caught:
        await store.put(object_key_for("toobig12"), chunked(data, 1000), max_bytes=4_096)
    assert caught.value.reason == "MEDIA_TOO_LARGE"


async def test_nothing_is_left_behind_after_an_oversized_stream(
    store: FilesystemObjectStore, tmp_path: Path
):
    with pytest.raises(errors.AppError):
        await store.put(object_key_for("toobig12"), chunked(b"x" * 10_000, 1000), max_bytes=4_096)

    assert await store.exists(object_key_for("toobig12")) is False
    assert list((tmp_path / "media" / ".tmp").glob("*.part")) == []


async def test_an_empty_stream_is_refused(store: FilesystemObjectStore):
    with pytest.raises(errors.AppError) as caught:
        await store.put(object_key_for("emptyone"), one(b""), max_bytes=GENEROUS)
    assert caught.value.reason == "MEDIA_EMPTY"


async def test_a_stream_exactly_at_the_limit_is_accepted(store: FilesystemObjectStore):
    data = b"x" * 4_096
    _, size = await store.put(object_key_for("exactfit"), chunked(data, 512), max_bytes=4_096)
    assert size == 4_096


async def test_a_failing_producer_leaves_no_object(store: FilesystemObjectStore, tmp_path: Path):
    """A client that disconnects half way through an upload.

    The partial write must not become a readable object, and must not linger.
    """

    async def breaks():
        yield b"first part"
        raise ConnectionError("the client went away")

    with pytest.raises(ConnectionError):
        await store.put(object_key_for("halfdone"), breaks(), max_bytes=GENEROUS)

    assert await store.exists(object_key_for("halfdone")) is False
    assert list((tmp_path / "media" / ".tmp").glob("*.part")) == []
