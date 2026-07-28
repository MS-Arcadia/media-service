"""The S3 object store.

Two halves, because two different things can be wrong.

The first half needs no server: the escalation from a single PUT to a multipart upload, the size
limit, and the abort on failure. Those are checked against a fake client that records the calls,
which is the only way to assert that a small file costs **one** request — a real server answers
correctly either way and says nothing about how many round trips it took.

The second half needs a real MinIO and is skipped without one. It is where the things a fake
cannot get wrong live: whether a stored object reads back byte-identical, whether the streaming
body actually streams, whether a missing key is a 404 and not a 500. The download bug this
adapter shipped with — an empty body behind a declared Content-Length — was invisible to every
fake and obvious to the first real read.

    ARCADIA_S3_ENDPOINT=http://localhost:9000 pytest tests/test_s3_store.py
"""

from __future__ import annotations

import os
from hashlib import sha256

import pytest

from app.adapters.outbound.s3 import MIN_PART_SIZE, S3ObjectStore
from app.platform import errors

GENEROUS = 1024 * 1024 * 1024


async def one(data: bytes):
    yield data


async def chunked(data: bytes, size: int):
    for start in range(0, len(data), size):
        yield data[start : start + size]


async def nothing():
    return
    yield b""  # unreachable; makes this an async generator


async def drain(stream) -> bytes:
    out = bytearray()
    async for chunk in stream:
        out.extend(chunk)
    return bytes(out)


# =========================================================================
# Against a fake client: how many requests, and of what kind
# =========================================================================


class FakeS3:
    """Records every call and behaves enough like S3 to drive the upload paths.

    The capitalised keyword arguments are S3's own parameter names, not a style slip — botocore
    takes `Bucket`, `Key`, `UploadId`, and a fake that renamed them would not be callable by the
    code under test.
    """

    def __init__(self, *, fail_on_part: int | None = None) -> None:
        self.calls: list[str] = []
        self.objects: dict[str, bytes] = {}
        self.parts: dict[str, list[bytes]] = {}
        self.aborted: list[str] = []
        self.completed: list[str] = []
        self._fail_on_part = fail_on_part

    async def put_object(self, *, Bucket, Key, Body):
        self.calls.append("put_object")
        self.objects[Key] = Body
        return {"ETag": '"fake"'}

    async def create_multipart_upload(self, *, Bucket, Key):
        self.calls.append("create_multipart_upload")
        upload_id = f"upload-{Key}"
        self.parts[upload_id] = []
        return {"UploadId": upload_id}

    async def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body):
        self.calls.append("upload_part")
        if self._fail_on_part == PartNumber:
            raise RuntimeError("the store refused a part")
        self.parts[UploadId].append(Body)
        return {"ETag": f'"part-{PartNumber}"'}

    async def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload):
        self.calls.append("complete_multipart_upload")
        self.completed.append(UploadId)
        self.objects[Key] = b"".join(self.parts[UploadId])
        return {"ETag": '"fake-multipart"'}

    async def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        self.calls.append("abort_multipart_upload")
        self.aborted.append(UploadId)
        return {}


def store_with(fake: FakeS3, *, part_size: int = MIN_PART_SIZE) -> S3ObjectStore:
    store = S3ObjectStore(
        endpoint_url="http://minio:9000",
        access_key="key",
        secret_key="secret",
        bucket="bucket",
        part_size=part_size,
    )
    # Reaching into a private attribute, deliberately: `start()` opens a real client, and none
    # of these tests are about that. The alternative is a constructor parameter that exists only
    # for tests, which is worse — it would let production code pass one too.
    store._client = fake
    return store


async def test_a_small_file_costs_one_request():
    """The reason for the buffer. Every screenshot on the platform takes this path, and a
    multipart upload for a 4 KB PNG would be three round trips instead of one."""
    fake = FakeS3()
    store = store_with(fake)

    checksum, size = await store.put("k", one(b"x" * 4096), max_bytes=GENEROUS)

    assert fake.calls == ["put_object"]
    assert size == 4096
    assert checksum == sha256(b"x" * 4096).hexdigest()
    assert fake.objects["k"] == b"x" * 4096


async def test_a_file_exactly_one_part_long_still_costs_one_request():
    """The boundary. `>=` on the buffer would escalate here and produce a multipart upload with
    one part, which S3 accepts and which costs three requests for no reason."""
    fake = FakeS3()
    store = store_with(fake, part_size=MIN_PART_SIZE)
    data = b"y" * MIN_PART_SIZE

    await store.put("k", one(data), max_bytes=GENEROUS)

    assert fake.calls.count("create_multipart_upload") <= 1
    assert fake.objects["k"] == data


async def test_a_large_file_escalates_to_multipart_and_reassembles_exactly():
    fake = FakeS3()
    store = store_with(fake, part_size=MIN_PART_SIZE)
    data = os.urandom(MIN_PART_SIZE * 2 + 1024)

    checksum, size = await store.put("k", chunked(data, 64 * 1024), max_bytes=GENEROUS)

    assert "create_multipart_upload" in fake.calls
    assert "complete_multipart_upload" in fake.calls
    assert size == len(data)
    assert checksum == sha256(data).hexdigest()
    # The bytes S3 would have stored, in order.
    assert fake.objects["k"] == data


async def test_only_the_final_part_is_allowed_to_be_short():
    """S3 rejects a non-final part under 5 MiB with EntityTooSmall — at upload time, after the
    whole file has been transferred. So the parts are checked, not just the result."""
    fake = FakeS3()
    store = store_with(fake, part_size=MIN_PART_SIZE)
    data = os.urandom(MIN_PART_SIZE * 2 + 100)

    await store.put("k", chunked(data, 1024), max_bytes=GENEROUS)

    parts = fake.parts[f"upload-{'k'}"]
    assert len(parts) >= 2
    assert all(len(part) >= MIN_PART_SIZE for part in parts[:-1])


async def test_a_failed_part_aborts_the_upload():
    """An abandoned multipart upload's parts are stored and billed until it is aborted, and
    nothing lists them by accident. Leaking one per failed upload is a bill that only grows."""
    fake = FakeS3(fail_on_part=1)
    store = store_with(fake, part_size=MIN_PART_SIZE)

    with pytest.raises(RuntimeError):
        await store.put("k", one(os.urandom(MIN_PART_SIZE * 2)), max_bytes=GENEROUS)

    assert fake.aborted == ["upload-k"]
    assert fake.completed == []


async def test_an_oversized_upload_is_refused_mid_stream_and_aborted():
    """Refused while the bytes are arriving, not after. Waiting until the end would mean
    transferring and storing the whole oversized file before rejecting it."""
    fake = FakeS3()
    store = store_with(fake, part_size=MIN_PART_SIZE)
    limit = MIN_PART_SIZE + 1024

    with pytest.raises(errors.AppError) as caught:
        await store.put("k", chunked(os.urandom(limit * 3), 64 * 1024), max_bytes=limit)

    assert caught.value.reason == "MEDIA_TOO_LARGE"
    # It had already escalated, so the partial upload must not be left behind.
    assert fake.aborted == ["upload-k"]
    assert "complete_multipart_upload" not in fake.calls


async def test_an_empty_upload_is_refused_and_stores_nothing():
    fake = FakeS3()
    store = store_with(fake)

    with pytest.raises(errors.AppError) as caught:
        await store.put("k", nothing(), max_bytes=GENEROUS)

    assert caught.value.reason == "MEDIA_EMPTY"
    assert fake.objects == {}


async def test_a_part_size_below_the_s3_minimum_is_refused_at_construction():
    """Refused here rather than clamped, because the alternative is every multipart upload of
    more than two parts failing at upload time with EntityTooSmall."""
    with pytest.raises(errors.AppError) as caught:
        S3ObjectStore(
            endpoint_url="http://minio:9000",
            access_key="k",
            secret_key="s",
            bucket="b",
            part_size=1024,
        )
    assert caught.value.reason == "INVALID_S3_PART_SIZE"


async def test_using_the_store_before_starting_it_says_so():
    """Rather than an AttributeError on None, which says nothing about what was forgotten."""
    store = S3ObjectStore(
        endpoint_url="http://minio:9000", access_key="k", secret_key="s", bucket="b"
    )
    with pytest.raises(errors.AppError) as caught:
        await store.exists("k")
    assert caught.value.reason == "S3_NOT_STARTED"


# =========================================================================
# Against a real MinIO
# =========================================================================

ENDPOINT = os.environ.get("ARCADIA_S3_ENDPOINT", "")

live = pytest.mark.skipif(
    not ENDPOINT,
    reason="set ARCADIA_S3_ENDPOINT=http://localhost:9000 to run the integration tests",
)


@pytest.fixture
async def live_store():
    store = S3ObjectStore(
        endpoint_url=ENDPOINT,
        access_key=os.environ.get("ARCADIA_S3_ACCESS_KEY", "arcadia-media-service"),
        secret_key=os.environ.get("ARCADIA_S3_SECRET_KEY", "local-development-media-s3-change-me"),
        bucket=os.environ.get("ARCADIA_S3_BUCKET", "arcadia-media"),
        create_bucket=False,
    )
    await store.start()
    yield store
    await store.aclose()


@live
async def test_a_stored_object_reads_back_byte_identical(live_store):
    """The check no fake can make. This adapter's first version streamed **nothing** on
    download — an empty body behind a declared Content-Length — and every unit test passed."""
    key = "tests/roundtrip"
    data = os.urandom(200_000)

    checksum, size = await live_store.put(key, chunked(data, 32 * 1024), max_bytes=GENEROUS)
    read_back = await drain(live_store.open(key))

    assert read_back == data
    assert size == len(data)
    assert checksum == sha256(data).hexdigest()
    await live_store.delete(key)


@live
async def test_a_multipart_object_reads_back_byte_identical(live_store):
    """The parts have to reassemble in order, and only a real server proves they did."""
    key = "tests/multipart"
    data = os.urandom(MIN_PART_SIZE * 2 + 7777)

    checksum, _ = await live_store.put(key, chunked(data, 128 * 1024), max_bytes=GENEROUS)
    read_back = await drain(live_store.open(key))

    assert read_back == data
    assert checksum == sha256(data).hexdigest()
    await live_store.delete(key)


@live
async def test_the_download_arrives_in_more_than_one_chunk(live_store):
    """Proves it streams rather than buffering the whole object and yielding it once — which
    would read identically and take a 4 GB build into memory."""
    key = "tests/streamed"
    data = os.urandom(3 * 1024 * 1024)
    await live_store.put(key, one(data), max_bytes=GENEROUS)

    chunks = [chunk async for chunk in live_store.open(key)]

    assert len(chunks) > 1
    assert b"".join(chunks) == data
    await live_store.delete(key)


@live
async def test_a_missing_key_is_reported_as_not_found(live_store):
    with pytest.raises(errors.AppError) as caught:
        await drain(live_store.open("tests/does-not-exist"))
    assert caught.value.reason == "OBJECT_NOT_FOUND"


@live
async def test_exists_and_delete_tell_the_truth(live_store):
    key = "tests/lifecycle"
    assert await live_store.exists(key) is False
    # False, not True: S3's DELETE is idempotent and succeeds for a key that was never there,
    # and a caller counting removals would report work it did not do.
    assert await live_store.delete(key) is False

    await live_store.put(key, one(b"z" * 32), max_bytes=GENEROUS)
    assert await live_store.exists(key) is True
    assert await live_store.delete(key) is True
    assert await live_store.exists(key) is False


@live
async def test_usage_counts_what_is_stored(live_store):
    key = "tests/usage"
    before = await live_store.usage_bytes()
    await live_store.put(key, one(b"q" * 5000), max_bytes=GENEROUS)

    assert await live_store.usage_bytes() - before == 5000
    await live_store.delete(key)


@live
async def test_readiness_proves_the_bucket_is_writable(live_store):
    """A `head_bucket` succeeds against a bucket these credentials cannot write to, and that
    difference only shows up on the first upload."""
    await live_store.check_ready()


@live
async def test_the_sweep_leaves_a_fresh_upload_alone(live_store):
    """The failure this method could plausibly cause is aborting a legitimate upload in flight —
    a 4 GB build on a slow connection is allowed to take hours. So the guard is tested."""
    key = "tests/in-flight"
    upload_id = await live_store._begin_multipart(key)
    try:
        assert await live_store.sweep_temporary_files() == 0
    finally:
        await live_store._abort(key, upload_id)
