"""The object store, on S3 — MinIO locally.

This is what `filesystem.py` said would be cheap to add, and it was: the use cases talk to
``ObjectStore`` and nothing above this file knows which of the two is running.

The point of it is not "S3 is better". It is that a filesystem store makes the service
**stateful**: two replicas on different hosts do not see each other's files, the store cannot
outgrow one disk, and it shares that disk with Postgres. With this adapter the service is
stateless again, which is the precondition for running more than one of it.

Three things here deserve explanation.

**Multipart, but only when it is needed.** S3 requires a `Content-Length` on a PUT, and an
upload arriving as a stream has no length until it ends. Buffering the whole thing to find out
would defeat the point. So the first ``part_size`` bytes are buffered: if the stream ends inside
that buffer — which is every screenshot — it goes as one ``put_object``. Only a genuinely large
file escalates to a multipart upload, and then no more than one part is ever in memory.

**The checksum is ours, not S3's.** ``ETag`` is an MD5 for a single PUT and something else
entirely for a multipart upload, so it cannot be compared against anything. The sha256 is
computed as the bytes pass through, exactly as the filesystem store does, and it means the same
thing whichever store is behind it.

**A failed multipart upload is not free.** Its parts are stored and billed until the upload is
aborted, and an aborted process leaves one behind with nothing referencing it. ``put`` aborts on
the way out of a failure, and ``sweep_temporary_files`` clears whatever a crash left — the same
job the filesystem store does for its ``.part`` files.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from aiobotocore.session import get_session
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from app.platform import errors

logger = logging.getLogger(__name__)

# 1 MiB, matching the filesystem store: large enough that a 4 GB download is not four million
# round trips, small enough that streaming one holds little memory per concurrent reader.
CHUNK_SIZE = 1024 * 1024

# The buffer that decides single-PUT versus multipart. S3's own minimum part size is 5 MiB for
# every part but the last, so this cannot go below that. 8 MiB keeps every screenshot and most
# trailers on the single-PUT path while bounding memory at one part per concurrent upload.
MIN_PART_SIZE = 5 * 1024 * 1024
DEFAULT_PART_SIZE = 8 * 1024 * 1024

# How long a multipart upload may sit unfinished before the sweep treats it as abandoned.
# Generous, because a legitimate 4 GB upload on a slow connection is allowed to take hours and
# aborting one in flight would fail a user's upload for no reason.
ABANDONED_UPLOAD_AGE = timedelta(hours=12)


class S3ObjectStore:
    """An ``ObjectStore`` backed by S3 or any API-compatible server."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
        part_size: int = DEFAULT_PART_SIZE,
        create_bucket: bool = True,
    ) -> None:
        if part_size < MIN_PART_SIZE:
            # Refused rather than clamped. A part size below S3's minimum makes every multipart
            # upload of more than two parts fail with EntityTooSmall, at upload time, on a
            # request that has already transferred the whole file.
            raise errors.internal(
                f"an S3 part size must be at least {MIN_PART_SIZE} bytes, not {part_size}",
                reason="INVALID_S3_PART_SIZE",
            )
        self._endpoint = endpoint_url.rstrip("/")
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._region = region
        self._part_size = part_size
        self._create_bucket = create_bucket
        self._stack = AsyncExitStack()
        self._client: Any = None

    # --- lifecycle -------------------------------------------------------

    async def start(self) -> None:
        """Open the client and make sure the bucket is there.

        Called from the lifespan rather than the constructor: creating the client is async, and
        a store that connects on first use would turn a misconfigured endpoint into a failed
        upload instead of a service that refuses to start.
        """
        if self._client is not None:
            return

        session = get_session()
        self._client = await self._stack.enter_async_context(
            session.create_client(
                "s3",
                endpoint_url=self._endpoint,
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                region_name=self._region,
                config=BotoConfig(
                    # Path style, not virtual-host style. `http://minio:9000/arcadia-media/key`
                    # resolves; `http://arcadia-media.minio:9000/key` needs DNS that does not
                    # exist on a Docker network, and the failure is a confusing name-resolution
                    # error rather than anything about S3.
                    s3={"addressing_style": "path"},
                    signature_version="s3v4",
                    # Bounded and few. A stalled store must surface as a failed request, not as
                    # a handler that never returns and a connection nobody gets back.
                    retries={"max_attempts": 3, "mode": "standard"},
                    connect_timeout=5,
                    read_timeout=60,
                    max_pool_connections=32,
                ),
            )
        )

        if self._create_bucket:
            await self._ensure_bucket()

        logger.info(
            "object store ready",
            extra={"backend": "s3", "endpoint": self._endpoint, "bucket": self._bucket},
        )

    async def aclose(self) -> None:
        await self._stack.aclose()
        self._client = None

    async def _ensure_bucket(self) -> None:
        try:
            await self._client.head_bucket(Bucket=self._bucket)
            return
        except ClientError as exc:
            if _status(exc) not in (404, 403):
                raise
            if _status(exc) == 403:
                # The bucket exists and these credentials may not look at it. Creating it is
                # not the fix and would fail anyway; say so plainly at boot.
                raise errors.internal(
                    f"the credentials may not access bucket {self._bucket}",
                    reason="S3_BUCKET_FORBIDDEN",
                ) from exc

        try:
            await self._client.create_bucket(Bucket=self._bucket)
            logger.info("created the object store bucket", extra={"bucket": self._bucket})
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise
            # Two replicas starting together both find it missing and both create it. One wins;
            # the loser's error is the answer it wanted.

    @property
    def _live(self) -> Any:
        if self._client is None:
            raise errors.internal(
                "the object store was used before it was started", reason="S3_NOT_STARTED"
            )
        return self._client

    # --- operations ------------------------------------------------------

    async def put(
        self, key: str, chunks: AsyncIterator[bytes], *, max_bytes: int
    ) -> tuple[str, int]:
        """Store a stream. Returns (sha256, size).

        Single PUT for anything that fits in one part, multipart above that. ``max_bytes`` is
        enforced as the bytes pass through, so an oversized upload is refused before the rest of
        it is transferred rather than after the whole file has been stored.
        """
        digest = sha256()
        size = 0
        buffer = bytearray()

        upload_id: str | None = None
        parts: list[dict[str, Any]] = []

        try:
            async for chunk in chunks:
                if not chunk:
                    continue
                size += len(chunk)
                if size > max_bytes:
                    raise errors.invalid_argument(
                        f"the upload exceeds the {max_bytes // (1024 * 1024)} MB limit for this "
                        f"kind of file",
                        reason="MEDIA_TOO_LARGE",
                        limit_bytes=max_bytes,
                    )
                digest.update(chunk)
                buffer.extend(chunk)

                # Only escalates once the buffer is genuinely full. A screenshot never gets
                # here, so the common upload costs one request.
                while len(buffer) >= self._part_size:
                    if upload_id is None:
                        upload_id = await self._begin_multipart(key)
                    part = bytes(buffer[: self._part_size])
                    del buffer[: self._part_size]
                    parts.append(await self._upload_part(key, upload_id, len(parts) + 1, part))

            if size == 0:
                raise errors.invalid_argument("the uploaded file is empty", reason="MEDIA_EMPTY")

            if upload_id is None:
                await self._live.put_object(Bucket=self._bucket, Key=key, Body=bytes(buffer))
            else:
                if buffer:
                    # The final part, and the only one allowed to be under S3's 5 MiB minimum.
                    parts.append(
                        await self._upload_part(key, upload_id, len(parts) + 1, bytes(buffer))
                    )
                await self._live.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
                upload_id = None
        except Exception:
            if upload_id is not None:
                # The parts already uploaded are stored and billed until this runs. Best effort:
                # a failing abort must not replace the real error with a cleanup error.
                await self._abort(key, upload_id)
            raise

        return digest.hexdigest(), size

    async def _begin_multipart(self, key: str) -> str:
        response = await self._live.create_multipart_upload(Bucket=self._bucket, Key=key)
        logger.debug("escalated an upload to multipart", extra={"key": key})
        return str(response["UploadId"])

    async def _upload_part(
        self, key: str, upload_id: str, number: int, body: bytes
    ) -> dict[str, Any]:
        response = await self._live.upload_part(
            Bucket=self._bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=number,
            Body=body,
        )
        return {"PartNumber": number, "ETag": response["ETag"]}

    async def _abort(self, key: str, upload_id: str) -> None:
        try:
            await self._live.abort_multipart_upload(
                Bucket=self._bucket, Key=key, UploadId=upload_id
            )
        except Exception:
            logger.warning(
                "could not abort a failed multipart upload; the sweep will clear it",
                extra={"key": key, "upload_id": upload_id},
            )

    def open(self, key: str) -> AsyncIterator[bytes]:
        """Stream the object back in chunks.

        Deliberately not ``async def``: an ``async def`` returning an async generator returns a
        coroutine wrapping one, and ``async for`` over the result fails. See ``ObjectStore.open``.
        """

        async def stream() -> AsyncIterator[bytes]:
            try:
                response = await self._live.get_object(Bucket=self._bucket, Key=key)
            except ClientError as exc:
                if _status(exc) == 404:
                    raise errors.not_found(
                        f"object {key} is not in the store", reason="OBJECT_NOT_FOUND"
                    ) from exc
                raise
            # The body is a stream, not bytes. Iterating it keeps one chunk in memory; calling
            # `.read()` would pull a 4 GB build into the process and take it down.
            #
            # `async with body:` with no `as`, which looks like a mistake and is not. The body's
            # `__aenter__` returns the underlying aiohttp `ClientResponse`, not the body — so
            # `async with response["Body"] as b` binds an object with no `iter_chunks` on it.
            # Written that way first, and the result was a download that streamed **nothing**
            # while still declaring a Content-Length: an empty file, no error in the log, and a
            # client failing on an incomplete read. The context manager is entered only for its
            # `__aexit__`, which is what releases the connection — there is no `close()`.
            body = response["Body"]
            async with body:
                async for chunk in body.iter_chunks(CHUNK_SIZE):
                    if chunk:
                        yield chunk

        return stream()

    async def delete(self, key: str) -> bool:
        # Checked first because S3's DELETE is idempotent and reports success for a key that was
        # never there. The caller uses the boolean to tell "removed" from "was not there", and
        # always answering True would make an orphan sweep report work it did not do.
        if not await self.exists(key):
            return False
        await self._live.delete_object(Bucket=self._bucket, Key=key)
        return True

    async def exists(self, key: str) -> bool:
        try:
            await self._live.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if _status(exc) in (403, 404):
                return False
            raise

    async def usage_bytes(self) -> int:
        """Total stored size, summed over every object.

        Paginated, because a bucket with more than a thousand objects returns a page at a time
        and a truncated listing would silently under-report usage — which, since a quota is
        enforced against this number, would silently raise the quota.

        Not on a request path: the readiness probe and the metrics gauge call it.
        """
        total = 0
        paginator = self._live.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self._bucket):
            for item in page.get("Contents", []):
                total += int(item.get("Size", 0))
        return total

    async def sweep_temporary_files(self) -> int:
        """Abort multipart uploads nobody is going to finish.

        The S3 equivalent of the filesystem store's leftover ``.part`` files, and it matters
        more here: an abandoned upload's parts are stored and billed indefinitely, and nothing
        lists them by accident. A crash mid-upload leaves exactly one.

        Only uploads older than ``ABANDONED_UPLOAD_AGE`` are touched. A legitimate 4 GB upload
        on a slow connection is allowed to take hours, and aborting one in flight would fail a
        user's upload for no reason — which is the failure this method could plausibly cause,
        so it is the one it is written to avoid.
        """
        cutoff = datetime.now(UTC) - ABANDONED_UPLOAD_AGE
        aborted = 0

        paginator = self._live.get_paginator("list_multipart_uploads")
        async for page in paginator.paginate(Bucket=self._bucket):
            for upload in page.get("Uploads", []):
                initiated = upload.get("Initiated")
                if initiated is not None and initiated > cutoff:
                    continue
                await self._abort(str(upload["Key"]), str(upload["UploadId"]))
                aborted += 1

        return aborted

    async def check_ready(self) -> None:
        """Prove the store is usable, not merely reachable.

        A `head_bucket` succeeds against a bucket these credentials cannot write to, and that
        difference only shows up on the first upload — which is exactly the failure readiness
        exists to catch first. So this writes a byte and removes it, the same thing the
        filesystem probe does for the same reason.
        """
        key = ".readyz"
        await self._live.put_object(Bucket=self._bucket, Key=key, Body=b"ok")
        await self._live.delete_object(Bucket=self._bucket, Key=key)


def _status(exc: ClientError) -> int:
    """The HTTP status behind a botocore error.

    Read from the response metadata rather than the error code string. MinIO and AWS do not
    always use the same code for the same condition — a missing key is `NoSuchKey` from one and
    `404` from the other on a HEAD, which has no body to carry a code at all — and the status is
    the part both agree on.
    """
    return int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
