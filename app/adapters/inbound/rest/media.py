"""Media endpoints.

The download route is the interesting one. It is the only route that accepts a signed token
instead of a bearer token, because a download usually is not made by an API client: it goes
into an `<img src>`, a `<video>`, or a download manager, none of which will attach an
`Authorization` header.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, File, Form, Query, Request, UploadFile, status
from fastapi.responses import StreamingResponse

from app.adapters.inbound.rest.deps import (
    CallerDep,
    MediaServiceDep,
    OptionalCallerDep,
    PageDep,
)
from app.application.dto import DownloadTicket, MediaView, Page
from app.domain.media import MAX_SIZE, MediaKind, Visibility
from app.platform import errors

router = APIRouter(prefix="/v1/media", tags=["media"])

# The largest thing any kind allows. The per-kind limit is checked in the domain; this is the
# outer bound, and it exists so that a request far beyond any limit is rejected without being
# read into memory first.
ABSOLUTE_MAX_BYTES = max(MAX_SIZE.values())


@router.post("", response_model=MediaView, status_code=status.HTTP_201_CREATED)
async def upload(
    service: MediaServiceDep,
    caller: CallerDep,
    request: Request,
    file: Annotated[UploadFile, File(description="The file to store")],
    kind: Annotated[MediaKind, Form()],
    reference_id: Annotated[str, Form(max_length=64)] = "",
    visibility: Annotated[Visibility | None, Form()] = None,
) -> MediaView:
    """Store a file.

    The declared content type is checked against the file's actual first bytes, so a `.png`
    that is really an HTML page is refused. Size limits are per kind — 10 MB for an image,
    4 GB for a game build.

    `visibility` may only make an object *more* restrictive. A game build cannot be made
    public: an unauthenticated URL for one is a pirated copy.
    """
    _reject_oversized(request)

    data = await file.read()
    return await service.upload(
        owner_id=caller.user_id,
        kind=kind,
        declared_type=file.content_type or "",
        data=data,
        filename=file.filename or "",
        reference_id=reference_id,
        visibility=visibility,
    )


@router.get("", response_model=Page[MediaView])
async def list_mine(
    service: MediaServiceDep,
    caller: CallerDep,
    page: PageDep,
    kind: Annotated[MediaKind | None, Query()] = None,
    include_deleted: Annotated[bool, Query()] = False,
) -> Page[MediaView]:
    return await service.list_mine(
        owner_id=caller.user_id,
        limit=page.limit,
        offset=page.offset,
        kind=kind,
        include_deleted=include_deleted,
    )


@router.get("/by-reference/{reference_id}", response_model=list[MediaView])
async def list_for_reference(service: MediaServiceDep, reference_id: str) -> list[MediaView]:
    """Public files attached to one game or post.

    How a storefront page fetches a game's screenshots. Public objects only, whoever asks —
    it must not leak the build sitting behind them.
    """
    return await service.list_for_reference(reference_id=reference_id)


@router.get("/{media_id}", response_model=MediaView)
async def describe(service: MediaServiceDep, caller: OptionalCallerDep, media_id: str) -> MediaView:
    """Metadata for one file.

    An object the caller may not read is reported as not found rather than forbidden.
    "Forbidden" confirms the id is real, which tells somebody enumerating ids that they have
    found an unreleased build.
    """
    return await service.describe(
        media_id=media_id,
        user_id=caller.user_id if caller else "",
        is_staff=bool(caller and caller.is_staff),
        has_read_scope=bool(caller and "media:read" in caller.scopes),
    )


@router.post("/{media_id}/ticket", response_model=DownloadTicket)
async def issue_ticket(
    service: MediaServiceDep, caller: CallerDep, media_id: str
) -> DownloadTicket:
    """Get a short-lived signed download URL — the local equivalent of an S3 presigned URL.

    Authorisation happens here, once, rather than on every byte of a download that may take
    twenty minutes. The returned URL carries its own proof and expires.
    """
    return await service.issue_ticket(
        media_id=media_id,
        user_id=caller.user_id,
        is_staff=caller.is_staff,
        has_read_scope="media:read" in caller.scopes,
    )


@router.get("/{media_id}/content")
async def download(
    service: MediaServiceDep,
    caller: OptionalCallerDep,
    media_id: str,
    token: Annotated[str, Query(max_length=1024)] = "",
) -> StreamingResponse:
    """Stream the bytes.

    Three ways to be allowed: the object is public, a valid signed token was presented, or the
    caller is the owner or staff. A public object needs no token — requiring one for a
    storefront screenshot would mean two round trips per image.
    """
    media, stream = await service.open_for_download(
        media_id=media_id,
        token=token,
        user_id=caller.user_id if caller else "",
        is_staff=bool(caller and caller.is_staff),
        has_read_scope=bool(caller and "media:read" in caller.scopes),
    )

    headers = {
        "Content-Length": str(media.size_bytes),
        # attachment, always. `inline` would let an uploaded SVG or HTML file execute in the
        # context of our own origin, which is a stored cross-site scripting hole.
        "Content-Disposition": _disposition(media.original_filename or media.id),
        # A browser that decides for itself what a file is undoes the type checking done at
        # upload.
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": (
            # A public asset is immutable — the id identifies these exact bytes — so it can be
            # cached hard. A private one must never be cached by a shared proxy.
            "public, max-age=31536000, immutable" if media.is_public else "private, no-store"
        ),
    }
    if media.checksum:
        headers["ETag"] = f'"{media.checksum}"'

    return StreamingResponse(stream, media_type=media.content_type, headers=headers)


@router.delete("/{media_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(service: MediaServiceDep, caller: CallerDep, media_id: str) -> None:
    """Remove the bytes, keep the record.

    A soft delete: a catalogue entry may still reference this id, and a hard delete would turn
    that into a dangling reference with nothing to explain it.
    """
    await service.delete(media_id=media_id, user_id=caller.user_id, is_staff=caller.is_staff)


def _reject_oversized(request: Request) -> None:
    """Refuse an obviously oversized upload from its Content-Length.

    A cheap first line only: the header is client-supplied and can lie, which is why the real
    check happens in the domain against the bytes actually read. This exists so the common
    honest case — somebody uploading a 6 GB file — is rejected before it is read.
    """
    raw = request.headers.get("content-length")
    if not raw:
        return
    try:
        declared = int(raw)
    except ValueError:
        return
    if declared > ABSOLUTE_MAX_BYTES:
        raise errors.invalid_argument(
            f"the request body is larger than the {ABSOLUTE_MAX_BYTES // (1024 * 1024)} MB "
            f"maximum for any file",
            reason="MEDIA_TOO_LARGE",
            limit_bytes=ABSOLUTE_MAX_BYTES,
        )


def _disposition(filename: str) -> str:
    """Build a Content-Disposition header safely.

    The filename has already been stripped of quotes, separators and control characters, so
    the quoted form cannot be broken out of. RFC 5987's `filename*` carries the UTF-8 version
    for anything non-ASCII.
    """
    from urllib.parse import quote

    ascii_name = filename.encode("ascii", "replace").decode("ascii")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename)}"
