"""The PostgreSQL media repository."""

from __future__ import annotations

from sqlalchemy import Select, func, select

from app.adapters.outbound.models import MediaRow
from app.domain.media import MediaKind, MediaObject, Visibility
from app.platform import errors
from app.platform.db import current_session


def _to_media(row: MediaRow) -> MediaObject:
    return MediaObject(
        id=row.id,
        owner_id=row.owner_id,
        kind=MediaKind(row.kind),
        visibility=Visibility(row.visibility),
        content_type=row.content_type,
        size_bytes=row.size_bytes,
        object_key=row.object_key,
        original_filename=row.original_filename,
        checksum=row.checksum,
        reference_id=row.reference_id,
        uploaded_at=row.uploaded_at,
        deleted_at=row.deleted_at,
    )


class PostgresMediaRepository:
    async def add(self, media: MediaObject) -> None:
        session = current_session()
        row = MediaRow(
            id=media.id,
            owner_id=media.owner_id,
            kind=str(media.kind),
            visibility=str(media.visibility),
            content_type=media.content_type,
            size_bytes=media.size_bytes,
            object_key=media.object_key,
            original_filename=media.original_filename,
            checksum=media.checksum,
            reference_id=media.reference_id,
            deleted_at=media.deleted_at,
        )
        if media.uploaded_at is not None:
            row.uploaded_at = media.uploaded_at
        session.add(row)
        await session.flush()

    async def get(self, media_id: str) -> MediaObject | None:
        session = current_session()
        row = await session.get(MediaRow, media_id)
        return _to_media(row) if row is not None else None

    async def get_for_update(self, media_id: str) -> MediaObject | None:
        session = current_session()
        row = (
            await session.execute(select(MediaRow).where(MediaRow.id == media_id).with_for_update())
        ).scalar_one_or_none()
        return _to_media(row) if row is not None else None

    async def save(self, media: MediaObject) -> None:
        session = current_session()
        row = await session.get(MediaRow, media.id)
        if row is None:
            raise errors.not_found(f"media {media.id} was not found")
        # Only the deletion timestamp and the checksum are mutable. Size, type and key are
        # facts about bytes that were already written; changing them would make the row
        # disagree with what is on disk.
        row.deleted_at = media.deleted_at
        row.checksum = media.checksum
        await session.flush()

    async def list_for_owner(
        self,
        owner_id: str,
        *,
        limit: int,
        offset: int,
        kind: MediaKind | None = None,
        include_deleted: bool = False,
    ) -> tuple[list[MediaObject], int]:
        stmt = select(MediaRow).where(MediaRow.owner_id == owner_id)
        if kind is not None:
            stmt = stmt.where(MediaRow.kind == str(kind))
        if not include_deleted:
            stmt = stmt.where(MediaRow.deleted_at.is_(None))
        stmt = stmt.order_by(MediaRow.uploaded_at.desc(), MediaRow.id)
        return await self._page(stmt, limit=limit, offset=offset)

    async def list_for_reference(self, reference_id: str) -> list[MediaObject]:
        session = current_session()
        rows = (
            (
                await session.execute(
                    select(MediaRow)
                    .where(MediaRow.reference_id == reference_id, MediaRow.deleted_at.is_(None))
                    .order_by(MediaRow.uploaded_at)
                    # Bounded even though the caller does not paginate: a reference with ten
                    # thousand files is a bug somewhere, and this endpoint should not become
                    # the way it takes the service down.
                    .limit(200)
                )
            )
            .scalars()
            .all()
        )
        return [_to_media(r) for r in rows]

    async def total_bytes(self) -> int:
        session = current_session()
        return int(
            (
                await session.execute(
                    select(func.coalesce(func.sum(MediaRow.size_bytes), 0)).where(
                        MediaRow.deleted_at.is_(None)
                    )
                )
            ).scalar()
            or 0
        )

    async def bytes_for_owner(self, owner_id: str) -> int:
        session = current_session()
        return int(
            (
                await session.execute(
                    select(func.coalesce(func.sum(MediaRow.size_bytes), 0)).where(
                        MediaRow.owner_id == owner_id,
                        MediaRow.deleted_at.is_(None),
                    )
                )
            ).scalar()
            or 0
        )

    async def _page(
        self, stmt: Select, *, limit: int, offset: int
    ) -> tuple[list[MediaObject], int]:
        session = current_session()
        total = int(
            (
                await session.execute(
                    select(func.count()).select_from(stmt.order_by(None).subquery())
                )
            ).scalar()
            or 0
        )
        rows = (await session.execute(stmt.limit(limit).offset(offset))).scalars().all()
        return [_to_media(r) for r in rows], total
