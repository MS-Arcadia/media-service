"""The use cases: upload, describe, download, delete.

One ordering decision runs through all of them: **the bytes are written before the metadata
row is committed.** If the write succeeds and the commit then fails, the result is an orphaned
file on disk — wasted space, found by a sweep. The other order would give a metadata row
pointing at a file that does not exist, which is a broken image in a storefront and a
download that 404s for something the catalogue says exists.

Wasted bytes are cheaper than a lie.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from app.application import events as ev
from app.application.download_token import TokenIssuer
from app.application.dto import DownloadTicket, MediaView, Page
from app.application.ports import (
    Clock,
    EventPublisher,
    IdFactory,
    MediaRepository,
    ObjectStore,
)
from app.domain import content
from app.domain.media import MediaKind, MediaObject, Visibility, validate_size
from app.platform import errors
from app.platform.db import UnitOfWork

logger = logging.getLogger(__name__)


class MediaService:
    def __init__(
        self,
        *,
        uow: UnitOfWork,
        repository: MediaRepository,
        store: ObjectStore,
        publisher: EventPublisher,
        tokens: TokenIssuer,
        clock: Clock,
        new_id: IdFactory,
        public_base_url: str,
    ) -> None:
        self._uow = uow
        self._repo = repository
        self._store = store
        self._publisher = publisher
        self._tokens = tokens
        self._clock = clock
        self._new_id = new_id
        self._public_base_url = public_base_url.rstrip("/")

    # --- upload ----------------------------------------------------------

    async def upload(
        self,
        *,
        owner_id: str,
        kind: MediaKind,
        declared_type: str,
        data: bytes,
        filename: str = "",
        reference_id: str = "",
        visibility: Visibility | None = None,
    ) -> MediaView:
        """Store a file.

        The size is checked before anything else, and again by the request-body limit at the
        edge — two layers, because the cheapest place to reject a 5 GB upload is before it
        has been read into memory, and this layer cannot be the only one that tries.
        """
        validate_size(kind=kind, size_bytes=len(data))

        # Identified from the bytes, not from what the uploader said they are.
        sniffed = content.identify(data[: content.SNIFF_BYTES], declared_type)

        media = MediaObject.create(
            media_id=self._new_id(),
            owner_id=owner_id,
            kind=kind,
            declared_type=declared_type,
            sniffed_type=sniffed,
            size_bytes=len(data),
            filename=filename,
            reference_id=reference_id,
            visibility=visibility,
            now=self._clock.now(),
        )

        # Bytes first. See the module docstring: an orphaned file is recoverable, a metadata
        # row pointing at nothing is not.
        media.checksum = await self._store.put(media.object_key, data)

        try:
            async with self._uow.begin() as session:
                await self._repo.add(media)
                await self._publisher.enqueue(
                    session,
                    event_type=ev.MEDIA_UPLOADED,
                    aggregate_type=ev.AGGREGATE_MEDIA,
                    aggregate_id=media.id,
                    payload=_payload(media),
                )
        except Exception:
            # The row did not land, so nothing references these bytes. Removing them here
            # turns the common failure into no leak at all; a crash before this line still
            # leaves an orphan for a sweep to find, which is the accepted cost.
            await self._store.delete(media.object_key)
            raise

        logger.info(
            "media stored",
            extra={
                "media_id": media.id,
                "kind": str(media.kind),
                "size_bytes": media.size_bytes,
                "content_type": media.content_type,
                "visibility": str(media.visibility),
            },
        )
        return self._view(media)

    # --- read ------------------------------------------------------------

    async def describe(
        self, *, media_id: str, user_id: str, is_staff: bool, has_read_scope: bool
    ) -> MediaView:
        media = await self._require(media_id)
        if not media.readable_by(user_id=user_id, is_staff=is_staff, has_read_scope=has_read_scope):
            # Not found rather than forbidden. "Forbidden" confirms the id is real, which
            # tells somebody enumerating ids that they have found an unreleased build.
            raise errors.not_found(f"media {media_id} was not found")
        return self._view(media)

    async def issue_ticket(
        self, *, media_id: str, user_id: str, is_staff: bool, has_read_scope: bool
    ) -> DownloadTicket:
        """Hand out a short-lived signed URL.

        Authorisation happens **here**, once, rather than on every byte of a download that
        may take twenty minutes. The token is the proof that it happened.
        """
        media = await self._require(media_id)
        if not media.readable_by(user_id=user_id, is_staff=is_staff, has_read_scope=has_read_scope):
            raise errors.not_found(f"media {media_id} was not found")

        now = self._clock.now()
        token = self._tokens.issue(media_id=media.id, subject=user_id or "anonymous", now=now)
        return DownloadTicket(
            media_id=media.id,
            url=f"{self._public_base_url}/v1/media/{media.id}/content?token={token}",
            expires_in_seconds=self._tokens.ttl_seconds,
            content_type=media.content_type,
            size_bytes=media.size_bytes,
            filename=media.original_filename,
        )

    async def open_for_download(
        self,
        *,
        media_id: str,
        token: str,
        user_id: str,
        is_staff: bool,
        has_read_scope: bool,
    ) -> tuple[MediaObject, AsyncIterator[bytes]]:
        """Authorise and open a stream.

        Three ways to be allowed, in the order they are cheapest to check: the object is
        public, a valid signed token was presented, or the caller is the owner or staff. A
        public object needs no token at all — that is what public means, and requiring one
        for a storefront screenshot would mean two round trips per image.
        """
        media = await self._require(media_id)

        if not media.is_public:
            if token:
                self._tokens.verify(token, media_id=media.id, now=self._clock.now())
            elif not media.readable_by(
                user_id=user_id, is_staff=is_staff, has_read_scope=has_read_scope
            ):
                raise errors.not_found(f"media {media_id} was not found")

        if not await self._store.exists(media.object_key):
            # The row says it exists and the bytes do not. Worth an ERROR: it means either
            # an interrupted delete or something outside this service touching the store.
            logger.error(
                "the metadata row has no bytes behind it",
                extra={"media_id": media.id, "object_key": media.object_key},
            )
            raise errors.internal("the stored file could not be read", reason="MEDIA_BYTES_MISSING")

        return media, self._store.open(media.object_key)

    async def list_mine(
        self,
        *,
        owner_id: str,
        limit: int,
        offset: int,
        kind: MediaKind | None = None,
        include_deleted: bool = False,
    ) -> Page[MediaView]:
        items, total = await self._repo.list_for_owner(
            owner_id, limit=limit, offset=offset, kind=kind, include_deleted=include_deleted
        )
        return Page(items=[self._view(m) for m in items], total=total, limit=limit, offset=offset)

    async def list_for_reference(self, *, reference_id: str) -> list[MediaView]:
        """Everything attached to one game or post.

        Only the public objects, whoever asks. This is how a storefront page fetches a
        game's screenshots, and it must not leak the build sitting behind them.
        """
        items = await self._repo.list_for_reference(reference_id)
        return [self._view(m) for m in items if m.is_public and not m.is_deleted]

    # --- delete ----------------------------------------------------------

    async def delete(self, *, media_id: str, user_id: str, is_staff: bool) -> None:
        """Remove the bytes; keep the record that they existed.

        A soft delete, because a catalogue entry or a community post may still reference this
        id. A hard delete would turn those into dangling references with nothing to say what
        they pointed at.

        The row is committed before the bytes are removed — the opposite order from upload,
        and for the same reason. If the delete of the bytes fails, the result is an orphaned
        file rather than a live reference to something already gone.
        """
        async with self._uow.begin() as session:
            media = await self._repo.get_for_update(media_id)
            if media is None:
                raise errors.not_found(f"media {media_id} was not found")
            media.assert_owner(user_id, is_staff=is_staff)

            if not media.delete(now=self._clock.now()):
                return

            await self._repo.save(media)
            await self._publisher.enqueue(
                session,
                event_type=ev.MEDIA_DELETED,
                aggregate_type=ev.AGGREGATE_MEDIA,
                aggregate_id=media.id,
                payload={
                    "media_id": media.id,
                    "owner_id": media.owner_id,
                    "reference_id": media.reference_id,
                    "kind": str(media.kind),
                },
            )

        removed = await self._store.delete(media.object_key)
        logger.info(
            "media deleted",
            extra={"media_id": media.id, "bytes_removed": removed},
        )

    # --- internals -------------------------------------------------------

    async def _require(self, media_id: str) -> MediaObject:
        media = await self._repo.get(media_id)
        if media is None:
            raise errors.not_found(f"media {media_id} was not found")
        media.assert_available()
        return media

    def _view(self, media: MediaObject) -> MediaView:
        return MediaView(
            id=media.id,
            owner_id=media.owner_id,
            kind=media.kind,
            visibility=media.visibility,
            content_type=media.content_type,
            size_bytes=media.size_bytes,
            filename=media.original_filename,
            checksum=media.checksum,
            reference_id=media.reference_id,
            uploaded_at=media.uploaded_at,
            deleted=media.is_deleted,
            # A direct URL only for public objects. Putting one on a private object would
            # invite a client to use it and get a 404 it cannot explain.
            url=(
                f"{self._public_base_url}/v1/media/{media.id}/content"
                if media.is_public and not media.is_deleted
                else ""
            ),
        )


def _payload(media: MediaObject) -> dict:
    return {
        "media_id": media.id,
        "owner_id": media.owner_id,
        "kind": str(media.kind),
        "visibility": str(media.visibility),
        "content_type": media.content_type,
        "size_bytes": media.size_bytes,
        "reference_id": media.reference_id,
        "checksum": media.checksum,
        "uploaded_at": media.uploaded_at.isoformat() if media.uploaded_at else None,
    }
