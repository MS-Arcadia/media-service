"""Request and response shapes."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.domain.media import MediaKind, Visibility


class Page[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int


class MediaView(BaseModel):
    id: str
    owner_id: str
    kind: MediaKind
    visibility: Visibility
    content_type: str
    size_bytes: int
    filename: str = ""
    checksum: str = ""
    reference_id: str = ""
    uploaded_at: datetime | None = None
    deleted: bool = False
    # Populated only for public objects. A private one needs a ticket, and offering a URL
    # that will 404 invites a client to use it.
    url: str = ""


class DownloadTicket(BaseModel):
    """A short-lived signed URL — the local equivalent of an S3 presigned URL."""

    media_id: str
    url: str
    expires_in_seconds: int
    content_type: str
    size_bytes: int
    filename: str = ""


class StorageStatsView(BaseModel):
    total_bytes: int
    object_count: int
