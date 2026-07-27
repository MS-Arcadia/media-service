"""The use cases, against an in-memory store.

The property these are really about is the ordering between the bytes and the metadata row:
an orphaned file is recoverable, a metadata row pointing at nothing is not.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from itertools import count

import pytest

from app.application import events as ev
from app.application.download_token import TokenIssuer
from app.application.media_service import MediaService
from app.domain.media import MediaKind, MediaObject, Visibility
from app.platform import errors

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
ZIP = b"PK\x03\x04" + b"\x00" * 100
HTML = b"<!DOCTYPE html><script>alert(1)</script>"


async def one(data: bytes) -> AsyncIterator[bytes]:
    """A single-chunk stream, for tests that do not care about chunking."""
    yield data


class FixedClock:
    def __init__(self) -> None:
        self._now = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs) -> datetime:
        self._now += timedelta(**kwargs)
        return self._now


class FakeUnitOfWork:
    """A transaction that does nothing — but insists on existing.

    The requirement that a scope is open is the point: the real repository takes its session
    from a context variable, so a use case that reads without opening one passes every unit
    test and then returns 500 in production. That happened, and it reached a container.
    """

    def __init__(self) -> None:
        self.fail_on_commit = False
        self.depth = 0

    @property
    def active(self) -> bool:
        return self.depth > 0

    @asynccontextmanager
    async def begin(self):
        self.depth += 1
        try:
            yield None
            if self.fail_on_commit:
                raise RuntimeError("the database went away at commit time")
        finally:
            self.depth -= 1

    @asynccontextmanager
    async def read(self):
        """A read scope. Nested inside begin(), it reuses it, like the real one."""
        self.depth += 1
        try:
            yield None
        finally:
            self.depth -= 1


def _require_scope(uow) -> None:
    """Mirror what platform.db.current_session does to a repository called out of scope."""
    if uow is not None and not uow.active:
        raise errors.internal(
            "no database session is active; repository calls must happen inside "
            "uow.begin() or uow.read()"
        )


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_on_put = False

    async def put(self, key: str, chunks, *, max_bytes: int) -> tuple[str, int]:
        if self.fail_on_put:
            raise RuntimeError("the disk is full")
        data = b"".join([chunk async for chunk in chunks])
        # Mirrors the real store's mid-stream enforcement, so a test that would have exceeded
        # the limit fails here too rather than only in production.
        if len(data) > max_bytes:
            raise errors.invalid_argument("too large", reason="MEDIA_TOO_LARGE")
        if not data:
            raise errors.invalid_argument("the uploaded file is empty", reason="MEDIA_EMPTY")
        self.objects[key] = data
        return sha256(data).hexdigest(), len(data)

    def open(self, key: str) -> AsyncIterator[bytes]:
        data = self.objects[key]

        async def stream() -> AsyncIterator[bytes]:
            yield data

        return stream()

    async def delete(self, key: str) -> bool:
        return self.objects.pop(key, None) is not None

    async def exists(self, key: str) -> bool:
        return key in self.objects

    async def usage_bytes(self) -> int:
        return sum(len(v) for v in self.objects.values())


class InMemoryRepository:
    def __init__(self, uow=None) -> None:
        self.items: dict[str, MediaObject] = {}
        self._uow = uow

    async def add(self, media: MediaObject) -> None:
        _require_scope(self._uow)
        self.items[media.id] = media

    async def get(self, media_id: str) -> MediaObject | None:
        _require_scope(self._uow)
        return self.items.get(media_id)

    async def get_for_update(self, media_id: str) -> MediaObject | None:
        _require_scope(self._uow)
        return self.items.get(media_id)

    async def save(self, media: MediaObject) -> None:
        _require_scope(self._uow)
        self.items[media.id] = media

    async def list_for_owner(
        self, owner_id: str, *, limit: int, offset: int, kind=None, include_deleted: bool = False
    ):
        _require_scope(self._uow)
        items = [m for m in self.items.values() if m.owner_id == owner_id]
        if kind is not None:
            items = [m for m in items if m.kind is kind]
        if not include_deleted:
            items = [m for m in items if not m.is_deleted]
        return items[offset : offset + limit], len(items)

    async def list_for_reference(self, reference_id: str):
        _require_scope(self._uow)
        return [m for m in self.items.values() if m.reference_id == reference_id]

    async def total_bytes(self) -> int:
        _require_scope(self._uow)
        return sum(m.size_bytes for m in self.items.values() if not m.is_deleted)

    async def bytes_for_owner(self, owner_id: str) -> int:
        _require_scope(self._uow)
        return sum(
            m.size_bytes for m in self.items.values() if m.owner_id == owner_id and not m.is_deleted
        )


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def enqueue(self, session, *, event_type, aggregate_type, aggregate_id, payload, **_):
        self.events.append({"event_type": event_type, "payload": payload})

    def types(self) -> list[str]:
        return [e["event_type"] for e in self.events]


class Harness:
    def __init__(
        self, *, storage_soft_limit_bytes: int = 10**12, owner_quota_bytes: int = 10**12
    ) -> None:
        self.clock = FixedClock()
        self.store = InMemoryStore()
        self.uow = FakeUnitOfWork()
        # Given the unit of work so it refuses a call made outside a scope, as the real one does.
        self.repo = InMemoryRepository(self.uow)
        self.publisher = RecordingPublisher()
        counter = count(1)
        self.service = MediaService(
            uow=self.uow,
            repository=self.repo,
            store=self.store,
            publisher=self.publisher,
            tokens=TokenIssuer("a-test-only-media-download-secret-32+"),
            clock=self.clock,
            new_id=lambda: f"media-{next(counter)}",
            public_base_url="http://localhost:8084",
            # Effectively unlimited by default, so the ordinary tests are unaffected by them.
            # The quota tests pass small numbers instead of uploading gigabytes.
            storage_soft_limit_bytes=storage_soft_limit_bytes,
            owner_quota_bytes=owner_quota_bytes,
        )

    async def upload_png(self, **kwargs):
        return await self.service.upload(
            owner_id=kwargs.pop("owner_id", "dev-1"),
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            chunks=one(PNG),
            filename="shot.png",
            **kwargs,
        )

    async def upload_build(self, **kwargs):
        return await self.service.upload(
            owner_id=kwargs.pop("owner_id", "dev-1"),
            kind=MediaKind.GAME_BINARY,
            declared_type="application/zip",
            chunks=one(ZIP),
            filename="game.zip",
            **kwargs,
        )


@pytest.fixture
def h() -> Harness:
    return Harness()


# --- upload --------------------------------------------------------------


async def test_an_upload_stores_the_bytes_and_the_metadata(h: Harness):
    view = await h.upload_png()

    assert view.content_type == "image/png"
    assert view.size_bytes == len(PNG)
    assert view.checksum == sha256(PNG).hexdigest()
    assert len(h.store.objects) == 1
    assert ev.MEDIA_UPLOADED in h.publisher.types()


async def test_an_html_file_declared_as_a_png_is_refused_and_stores_nothing(h: Harness):
    with pytest.raises(errors.AppError) as caught:
        await h.service.upload(
            owner_id="dev-1",
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            chunks=one(HTML),
            filename="evil.png",
        )
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"
    assert h.store.objects == {}
    assert h.repo.items == {}


async def test_a_failed_byte_write_stores_no_metadata(h: Harness):
    """The row must never reference bytes that were never written."""
    h.store.fail_on_put = True
    with pytest.raises(RuntimeError):
        await h.upload_png()
    assert h.repo.items == {}


async def test_a_failed_commit_removes_the_orphaned_bytes(h: Harness):
    """Bytes are written first, so a failed commit leaves them unreferenced.

    Cleaning up here turns the common failure into no leak at all. A crash before the cleanup
    still leaves an orphan for the boot-time sweep, which is the accepted cost of this
    ordering.
    """
    h.uow.fail_on_commit = True
    with pytest.raises(RuntimeError):
        await h.upload_png()
    assert h.store.objects == {}


async def test_a_public_upload_gets_a_direct_url(h: Harness):
    view = await h.upload_png()
    assert view.url.endswith(f"/v1/media/{view.id}/content")


async def test_a_private_upload_gets_no_direct_url(h: Harness):
    """Offering a URL that will 404 invites a client to use it."""
    view = await h.upload_build()
    assert view.visibility is Visibility.PRIVATE
    assert view.url == ""


# --- reading -------------------------------------------------------------


async def test_a_public_object_can_be_described_anonymously(h: Harness):
    view = await h.upload_png()
    described = await h.service.describe(
        media_id=view.id, user_id="", is_staff=False, has_read_scope=False
    )
    assert described.id == view.id


async def test_a_private_object_is_reported_as_not_found_to_a_stranger(h: Harness):
    """Not "forbidden": that would confirm the id is real, telling somebody enumerating ids
    they have found an unreleased build."""
    view = await h.upload_build()
    with pytest.raises(errors.AppError) as caught:
        await h.service.describe(
            media_id=view.id, user_id="stranger", is_staff=False, has_read_scope=False
        )
    assert caught.value.http_status == 404


async def test_the_owner_can_describe_their_own_private_object(h: Harness):
    view = await h.upload_build()
    assert await h.service.describe(
        media_id=view.id, user_id="dev-1", is_staff=False, has_read_scope=False
    )


# --- download ------------------------------------------------------------


async def test_a_public_object_downloads_with_no_token(h: Harness):
    """Requiring one for a storefront screenshot would mean two round trips per image."""
    view = await h.upload_png()
    media, stream = await h.service.open_for_download(
        media_id=view.id, token="", user_id="", is_staff=False, has_read_scope=False
    )
    assert media.id == view.id
    assert b"".join([chunk async for chunk in stream]) == PNG


async def test_a_private_object_needs_a_token_or_ownership(h: Harness):
    view = await h.upload_build()
    with pytest.raises(errors.AppError) as caught:
        await h.service.open_for_download(
            media_id=view.id, token="", user_id="stranger", is_staff=False, has_read_scope=False
        )
    assert caught.value.http_status == 404


async def test_a_ticket_lets_a_private_object_be_downloaded(h: Harness):
    view = await h.upload_build()
    ticket = await h.service.issue_ticket(
        media_id=view.id, user_id="dev-1", is_staff=False, has_read_scope=False
    )
    token = ticket.url.split("token=")[1]

    media, stream = await h.service.open_for_download(
        media_id=view.id, token=token, user_id="", is_staff=False, has_read_scope=False
    )
    assert b"".join([chunk async for chunk in stream]) == ZIP
    assert media.visibility is Visibility.PRIVATE


async def test_a_ticket_for_one_file_cannot_download_another(h: Harness):
    shot = await h.upload_png()
    build = await h.upload_build()
    ticket = await h.service.issue_ticket(
        media_id=shot.id, user_id="dev-1", is_staff=False, has_read_scope=False
    )
    token = ticket.url.split("token=")[1]

    with pytest.raises(errors.AppError) as caught:
        await h.service.open_for_download(
            media_id=build.id, token=token, user_id="", is_staff=False, has_read_scope=False
        )
    assert caught.value.reason == "DOWNLOAD_TOKEN_INVALID"


async def test_an_expired_ticket_stops_working(h: Harness):
    view = await h.upload_build()
    ticket = await h.service.issue_ticket(
        media_id=view.id, user_id="dev-1", is_staff=False, has_read_scope=False
    )
    token = ticket.url.split("token=")[1]
    h.clock.advance(hours=1)

    with pytest.raises(errors.AppError) as caught:
        await h.service.open_for_download(
            media_id=view.id, token=token, user_id="", is_staff=False, has_read_scope=False
        )
    assert caught.value.reason == "DOWNLOAD_TOKEN_EXPIRED"


async def test_a_stranger_cannot_get_a_ticket_for_a_private_object(h: Harness):
    """Authorisation happens when the ticket is issued, once, rather than on every byte."""
    view = await h.upload_build()
    with pytest.raises(errors.AppError) as caught:
        await h.service.issue_ticket(
            media_id=view.id, user_id="stranger", is_staff=False, has_read_scope=False
        )
    assert caught.value.http_status == 404


async def test_missing_bytes_behind_a_live_row_are_reported_as_a_server_error(h: Harness):
    """It means an interrupted delete, or something outside this service touching the store.
    Either way it is our fault, not the caller's."""
    view = await h.upload_png()
    h.store.objects.clear()

    with pytest.raises(errors.AppError) as caught:
        await h.service.open_for_download(
            media_id=view.id, token="", user_id="", is_staff=False, has_read_scope=False
        )
    assert caught.value.reason == "MEDIA_BYTES_MISSING"
    assert caught.value.http_status == 500


# --- listing by reference -----------------------------------------------


async def test_listing_by_reference_returns_only_public_files(h: Harness):
    """How a storefront page fetches a game's screenshots. It must not leak the build sitting
    behind them."""
    await h.upload_png(reference_id="game-1")
    await h.upload_build(reference_id="game-1")

    items = await h.service.list_for_reference(reference_id="game-1")

    assert len(items) == 1
    assert items[0].kind is MediaKind.IMAGE


async def test_listing_by_reference_omits_deleted_files(h: Harness):
    view = await h.upload_png(reference_id="game-1")
    await h.service.delete(media_id=view.id, user_id="dev-1", is_staff=False)
    assert await h.service.list_for_reference(reference_id="game-1") == []


# --- delete --------------------------------------------------------------


async def test_deleting_removes_the_bytes_but_keeps_the_record(h: Harness):
    """A catalogue entry may still reference this id; a hard delete would leave a dangling
    reference with nothing to explain it."""
    view = await h.upload_png()

    await h.service.delete(media_id=view.id, user_id="dev-1", is_staff=False)

    assert h.store.objects == {}
    assert h.repo.items[view.id].is_deleted is True
    assert ev.MEDIA_DELETED in h.publisher.types()


async def test_a_deleted_object_is_no_longer_readable(h: Harness):
    view = await h.upload_png()
    await h.service.delete(media_id=view.id, user_id="dev-1", is_staff=False)

    with pytest.raises(errors.AppError) as caught:
        await h.service.describe(
            media_id=view.id, user_id="dev-1", is_staff=False, has_read_scope=False
        )
    assert caught.value.reason == "MEDIA_DELETED"


async def test_a_stranger_cannot_delete_someone_elses_file(h: Harness):
    view = await h.upload_png()
    with pytest.raises(errors.AppError) as caught:
        await h.service.delete(media_id=view.id, user_id="stranger", is_staff=False)
    assert caught.value.reason == "NOT_MEDIA_OWNER"
    assert h.store.objects != {}


async def test_staff_can_delete_reported_content(h: Harness):
    view = await h.upload_png()
    await h.service.delete(media_id=view.id, user_id="support-1", is_staff=True)
    assert h.repo.items[view.id].is_deleted is True


async def test_deleting_twice_publishes_only_one_event(h: Harness):
    view = await h.upload_png()
    await h.service.delete(media_id=view.id, user_id="dev-1", is_staff=False)
    await h.service.delete(media_id=view.id, user_id="dev-1", is_staff=False)
    assert h.publisher.types().count(ev.MEDIA_DELETED) == 1


# --- streaming -----------------------------------------------------------


async def test_the_type_is_decided_from_the_first_chunk_alone(h: Harness):
    """So a rejected upload costs one buffer rather than a whole file on disk."""

    async def in_pieces():
        yield PNG[:8]
        yield PNG[8:]

    view = await h.service.upload(
        owner_id="dev-1",
        kind=MediaKind.IMAGE,
        declared_type="image/png",
        chunks=in_pieces(),
        filename="split.png",
    )
    assert view.content_type == "image/png"
    assert view.size_bytes == len(PNG)


async def test_a_bad_type_is_rejected_before_anything_is_written(h: Harness):
    """The signature check happens before the first write, so the disk is never touched."""

    async def in_pieces():
        yield HTML[:8]
        yield HTML[8:]

    with pytest.raises(errors.AppError) as caught:
        await h.service.upload(
            owner_id="dev-1",
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            chunks=in_pieces(),
        )
    assert caught.value.reason == "CONTENT_TYPE_MISMATCH"
    assert h.store.objects == {}


async def test_an_empty_stream_is_refused_before_the_store_is_touched(h: Harness):
    async def nothing():
        return
        yield b""  # pragma: no cover

    with pytest.raises(errors.AppError) as caught:
        await h.service.upload(
            owner_id="dev-1",
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            chunks=nothing(),
        )
    assert caught.value.reason == "MEDIA_EMPTY"
    assert h.store.objects == {}


async def test_the_stores_size_limit_is_the_one_that_applies(h: Harness):
    """The size is not knowable in advance from anything trustworthy, so the store enforces it
    as the bytes pass through — and the use case passes the limit for the kind."""
    from app.domain.media import MAX_SIZE

    oversized = PNG[:8] + b"\x00" * MAX_SIZE[MediaKind.IMAGE]
    with pytest.raises(errors.AppError) as caught:
        await h.service.upload(
            owner_id="dev-1",
            kind=MediaKind.IMAGE,
            declared_type="image/png",
            chunks=one(oversized),
        )
    assert caught.value.reason == "MEDIA_TOO_LARGE"


# --- quotas -------------------------------------------------------------
#
# Two separate limits, and each test names which hole it closes.


async def test_an_owner_over_their_quota_cannot_upload():
    """The point of the per-owner limit.

    Without it, one developer uploading builds in a loop reaches the global limit on their own
    and every other developer on the platform is unable to publish.
    """
    h = Harness(owner_quota_bytes=len(PNG) + 10)
    await h.upload_png(owner_id="dev-1")

    with pytest.raises(errors.AppError) as caught:
        await h.upload_png(owner_id="dev-1")
    assert caught.value.reason == "OWNER_QUOTA_EXCEEDED"
    assert caught.value.details["quota_bytes"] == len(PNG) + 10


async def test_one_owners_quota_does_not_affect_another():
    """A quota is a fair share, so it has to be counted per owner rather than in total."""
    h = Harness(owner_quota_bytes=len(PNG) + 10)
    await h.upload_png(owner_id="dev-1")

    view = await h.upload_png(owner_id="dev-2")
    assert view.id


async def test_deleting_gives_the_space_back():
    """A developer who withdrew a build has given the space back.

    Counting deleted media against them would make the quota impossible to get under, which is
    the kind of bug that only shows up once someone actually hits the limit.
    """
    h = Harness(owner_quota_bytes=len(PNG) + 10)
    first = await h.upload_png(owner_id="dev-1")
    await h.service.delete(media_id=first.id, user_id="dev-1", is_staff=False)

    view = await h.upload_png(owner_id="dev-1")
    assert view.id


async def test_a_rejected_upload_leaves_no_bytes_behind():
    """The size is only known after the write, so the second check runs with the file already on
    disk. If the cleanup did not happen, every refused upload would leak the very space the
    quota exists to protect."""
    h = Harness(owner_quota_bytes=len(PNG) + 10)
    await h.upload_png(owner_id="dev-1")
    stored_before = dict(h.store.objects)

    with pytest.raises(errors.AppError):
        await h.upload_png(owner_id="dev-1")
    assert h.store.objects == stored_before


async def test_the_global_soft_limit_stops_uploads_from_everyone():
    """Reported as unavailable, not as a bad request: the uploader did nothing wrong, and once
    an operator reclaims space the identical request succeeds."""
    h = Harness(storage_soft_limit_bytes=len(PNG) + 10)
    await h.upload_png(owner_id="dev-1")

    with pytest.raises(errors.AppError) as caught:
        await h.upload_png(owner_id="dev-2")
    assert caught.value.reason == "STORAGE_SOFT_LIMIT_REACHED"
    assert caught.value.code is errors.Code.UNAVAILABLE


async def test_an_owner_already_over_quota_is_refused_before_the_bytes_are_read():
    """An upload that cannot be kept should not spend twenty minutes of bandwidth first.

    Proven by the store never being written to, not by timing.
    """
    h = Harness(owner_quota_bytes=1)
    with pytest.raises(errors.AppError) as caught:
        await h.upload_png(owner_id="dev-1")
    assert caught.value.reason == "OWNER_QUOTA_EXCEEDED"
    assert h.store.objects == {}
