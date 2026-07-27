"""The interfaces the use cases depend on.

``ObjectStore`` is the one that matters. The architecture document specifies MinIO; this
runs on the local filesystem. Because the use cases only ever see this protocol, swapping in
an S3 or MinIO adapter is a new file in ``adapters/outbound`` and one line in
``bootstrap.py`` — nothing above this line changes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Protocol

from app.domain.media import MediaKind, MediaObject


class Clock(Protocol):
    def now(self) -> datetime: ...


class IdFactory(Protocol):
    def __call__(self) -> str: ...


class ObjectStore(Protocol):
    """Where the bytes live."""

    async def put(self, key: str, data: bytes) -> str:
        """Store the bytes and return their checksum."""
        ...

    def open(self, key: str) -> AsyncIterator[bytes]:
        """Stream the bytes back in chunks.

        Streaming rather than returning them whole: a 4 GB game build read into memory would
        take the process down, and would do it once per concurrent download.

        Deliberately **not** ``async def``. An ``async def`` that returns an async generator
        returns a *coroutine wrapping* one, so ``async for`` over the result fails — the
        caller would have to await it first, which reads like a bug even when it is correct.
        A plain method returning an async iterator is the idiom that composes.
        """
        ...

    async def delete(self, key: str) -> bool: ...

    async def exists(self, key: str) -> bool: ...

    async def usage_bytes(self) -> int:
        """Total stored size, for the readiness probe and the metrics."""
        ...


class MediaRepository(Protocol):
    async def add(self, media: MediaObject) -> None: ...

    async def get(self, media_id: str) -> MediaObject | None: ...

    async def get_for_update(self, media_id: str) -> MediaObject | None: ...

    async def save(self, media: MediaObject) -> None: ...

    async def list_for_owner(
        self,
        owner_id: str,
        *,
        limit: int,
        offset: int,
        kind: MediaKind | None = None,
        include_deleted: bool = False,
    ) -> tuple[list[MediaObject], int]: ...

    async def list_for_reference(self, reference_id: str) -> list[MediaObject]: ...

    async def total_bytes(self) -> int: ...


class EventPublisher(Protocol):
    async def enqueue(
        self,
        session: Any,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        topic: str = "",
        causation_id: str = "",
    ) -> None: ...
