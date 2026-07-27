"""SQLAlchemy tables.

One table. This service stores bytes and the minimum needed to find them again; anything
about what a file *means* — which game it illustrates, which post it belongs to — is the
catalogue's or the community's business, and is referenced here only as an opaque id.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.platform.db import Base


class MediaRow(Base):
    __tablename__ = "media_objects"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Derived from the id, never from the uploaded filename.
    object_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    checksum: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    reference_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    uploaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Soft delete: a catalogue entry may still reference this id, and a hard delete would
    # turn that into a dangling reference with nothing to explain it.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_media_owner_kind", "owner_id", "kind"),
        # How a storefront page fetches a game's screenshots.
        Index("ix_media_reference", "reference_id"),
    )
