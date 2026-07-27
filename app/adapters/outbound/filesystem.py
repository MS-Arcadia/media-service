"""The object store, on the local filesystem.

The architecture document specifies MinIO. This runs on a directory instead, for one reason:
MinIO is another container, another 300 MB of memory and another credential to manage, and
the platform is being run on plain Docker with a stated shortage of resources.

The trade-off is real and worth stating. A filesystem store means the service is **not**
stateless: two replicas do not see each other's files unless they share a volume, and there
is no replication. That is acceptable for a single-replica local and demo deployment and is
not acceptable in production.

It is also cheap to fix. Everything above this file talks to ``ObjectStore``, so an S3 or
MinIO adapter is a new file next to this one and one line in ``bootstrap.py``. Nothing in the
domain or the use cases changes.

Writes stream to a temporary file and are then renamed. ``os.replace`` is atomic within a
filesystem, so a crash mid-write leaves a temporary file to clean up rather than a
half-written object that reads as valid. Nothing here ever holds a whole file in memory.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from hashlib import sha256
from pathlib import Path

from app.platform import errors

logger = logging.getLogger(__name__)

# 1 MiB. Large enough that a 4 GB file is not four million syscalls, small enough that
# streaming one does not hold much memory per concurrent download.
CHUNK_SIZE = 1024 * 1024


class FilesystemObjectStore:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        self._tmp = self._root / ".tmp"
        self._root.mkdir(parents=True, exist_ok=True)
        self._tmp.mkdir(parents=True, exist_ok=True)

    # --- paths -----------------------------------------------------------

    def _path(self, key: str) -> Path:
        """Resolve a key to a path, refusing anything that escapes the root.

        The key is generated from a media id and never contains user input, so this cannot
        trigger in normal operation. It is here because "cannot happen" is a statement about
        today's callers: a future one that builds a key differently should fail loudly rather
        than write outside the store.
        """
        candidate = (self._root / key).resolve()
        if not candidate.is_relative_to(self._root):
            raise errors.internal(
                f"object key {key!r} resolves outside the store", reason="INVALID_OBJECT_KEY"
            )
        return candidate

    # --- operations ------------------------------------------------------

    async def put(
        self, key: str, chunks: AsyncIterator[bytes], *, max_bytes: int
    ) -> tuple[str, int]:
        """Write a stream atomically. Returns (sha256, size).

        The whole file never exists in memory: chunks go straight to a temporary file, and the
        checksum and size are accumulated as they pass through.

        Every blocking call runs in a worker thread. Doing this on the event loop would stall
        every other request in the process for the duration of the upload.
        """
        target = self._path(key)
        temp = self._tmp / f"{key.replace('/', '_')}.part"

        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(temp.parent.mkdir, parents=True, exist_ok=True)

        digest = sha256()
        size = 0
        handle = await asyncio.to_thread(temp.open, "wb")
        try:
            async for chunk in chunks:
                if not chunk:
                    continue
                size += len(chunk)
                if size > max_bytes:
                    # Refused mid-stream. Waiting until the end would mean writing the whole
                    # oversized file to disk before rejecting it, which is exactly the
                    # resource exhaustion the limit exists to prevent.
                    raise errors.invalid_argument(
                        f"the upload exceeds the {max_bytes // (1024 * 1024)} MB limit for this "
                        f"kind of file",
                        reason="MEDIA_TOO_LARGE",
                        limit_bytes=max_bytes,
                    )
                digest.update(chunk)
                await asyncio.to_thread(handle.write, chunk)

            await asyncio.to_thread(handle.flush)
            # Forced to disk before the rename. Without it a power loss can leave the rename
            # durable and the contents not, which is a file that exists and is empty.
            await asyncio.to_thread(os.fsync, handle.fileno())
            await asyncio.to_thread(handle.close)
            handle = None  # type: ignore[assignment]

            if size == 0:
                raise errors.invalid_argument("the uploaded file is empty", reason="MEDIA_EMPTY")

            # Atomic within a filesystem, so a crash mid-write leaves a temporary file to
            # clean up rather than a half-written object that reads as valid.
            await asyncio.to_thread(os.replace, temp, target)
        finally:
            if handle is not None:
                await asyncio.to_thread(handle.close)
            await asyncio.to_thread(temp.unlink, True)

        return digest.hexdigest(), size

    def open(self, key: str) -> AsyncIterator[bytes]:
        """Open a stream over the object.

        Not ``async def``: see the note on ``ObjectStore.open``. The caller checks existence
        first, so a file that disappears between the two raises on the first read — which is
        the honest outcome and cannot be prevented anyway.
        """
        path = self._path(key)

        async def stream() -> AsyncIterator[bytes]:
            handle = await asyncio.to_thread(path.open, "rb")
            try:
                while True:
                    chunk = await asyncio.to_thread(handle.read, CHUNK_SIZE)
                    if not chunk:
                        return
                    yield chunk
            finally:
                await asyncio.to_thread(handle.close)

        return stream()

    async def delete(self, key: str) -> bool:
        def remove() -> bool:
            path = self._path(key)
            if not path.exists():
                return False
            path.unlink()
            # Tidy the shard directories, but only while they are empty. Left alone they
            # accumulate until a store with a long history is mostly empty directories.
            for parent in (path.parent, path.parent.parent):
                try:
                    if parent != self._root and not any(parent.iterdir()):
                        parent.rmdir()
                except OSError:
                    break
            return True

        return await asyncio.to_thread(remove)

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(lambda: self._path(key).is_file())

    async def usage_bytes(self) -> int:
        """Total stored size.

        Walks the tree, so it is not free. Called by the readiness probe and the metrics, not
        on a request path.
        """

        def walk() -> int:
            total = 0
            for path in self._root.rglob("*"):
                if path.is_file() and ".tmp" not in path.parts:
                    total += path.stat().st_size
            return total

        return await asyncio.to_thread(walk)

    async def sweep_temporary_files(self) -> int:
        """Remove leftover partial writes.

        A crash between opening the temporary file and the rename leaves one behind. Nothing
        references it, so it is pure waste — and without a sweep it is waste that only grows.
        """

        def sweep() -> int:
            removed = 0
            for path in self._tmp.glob("*.part"):
                try:
                    path.unlink()
                    removed += 1
                except OSError:
                    logger.warning("could not remove a partial upload", extra={"path": str(path)})
            return removed

        return await asyncio.to_thread(sweep)
