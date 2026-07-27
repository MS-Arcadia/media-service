"""The MediaObject aggregate and the policy that decides what may be stored.

This service is the platform's only file store, which makes it the platform's largest
attack surface for content. Every rule here exists because of a specific way that goes
wrong:

* a **size limit per kind**, because one 40 GB upload fills a disk that four other services
  share;
* a **content-type allowlist**, because "any file" means an HTML page served from our own
  origin, or a script somebody hopes will be executed;
* the declared type is **verified against the bytes**, because the declared type is supplied
  by the uploader and is therefore worthless on its own;
* **visibility**, because a game's build is not a screenshot: one is a public asset, the
  other is the thing people paid for.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from app.platform import errors

MB = 1024 * 1024


class MediaKind(StrEnum):
    """What the file is for. Determines its limits and who may read it."""

    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    FILE = "FILE"
    GAME_BINARY = "GAME_BINARY"


class Visibility(StrEnum):
    """Who may read the bytes.

    PUBLIC is for storefront assets — a teaser, a screenshot — which are meant to be seen
    by anyone browsing the catalogue, including people who are not logged in.

    PRIVATE is for anything that was paid for or is not yet published. A game build is
    private: an unauthenticated download URL for it is a pirated copy.
    """

    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"


# Size ceilings per kind. These are blast-radius limits, not business rules: they stop one
# upload exhausting shared disk, and they are deliberately generous rather than tuned.
MAX_SIZE: dict[MediaKind, int] = {
    MediaKind.IMAGE: 10 * MB,
    MediaKind.VIDEO: 200 * MB,
    MediaKind.FILE: 50 * MB,
    MediaKind.GAME_BINARY: 4096 * MB,
}

# An allowlist, never a denylist. A denylist is a list of the attacks somebody thought of.
ALLOWED_TYPES: dict[MediaKind, frozenset[str]] = {
    MediaKind.IMAGE: frozenset({"image/png", "image/jpeg", "image/webp", "image/gif"}),
    MediaKind.VIDEO: frozenset({"video/mp4", "video/webm"}),
    MediaKind.FILE: frozenset({"application/pdf", "text/plain", "application/zip"}),
    MediaKind.GAME_BINARY: frozenset(
        {"application/zip", "application/x-7z-compressed", "application/octet-stream"}
    ),
}

# What each kind defaults to. A game build defaulting to public would be the single worst
# bug this service could have, so the default is derived from the kind rather than taken
# from the request.
DEFAULT_VISIBILITY: dict[MediaKind, Visibility] = {
    MediaKind.IMAGE: Visibility.PUBLIC,
    MediaKind.VIDEO: Visibility.PUBLIC,
    MediaKind.FILE: Visibility.PRIVATE,
    MediaKind.GAME_BINARY: Visibility.PRIVATE,
}

FILENAME_MAX = 255

REASON_TOO_LARGE = "MEDIA_TOO_LARGE"
REASON_EMPTY = "MEDIA_EMPTY"
REASON_TYPE_NOT_ALLOWED = "CONTENT_TYPE_NOT_ALLOWED"
REASON_TYPE_MISMATCH = "CONTENT_TYPE_MISMATCH"
REASON_NOT_OWNER = "NOT_MEDIA_OWNER"
REASON_DELETED = "MEDIA_DELETED"
REASON_FILENAME = "FILENAME_INVALID"


@dataclass(slots=True)
class MediaObject:
    """One stored file.

    ``object_key`` is derived from the id, never from the uploaded filename. That is what
    makes path traversal structurally impossible rather than merely filtered: there is no
    code path in which user input reaches the filesystem as a path.
    """

    id: str
    owner_id: str
    kind: MediaKind
    visibility: Visibility
    content_type: str
    size_bytes: int
    object_key: str
    # The name the uploader used, kept only to offer it back on download. Never used to
    # build a path.
    original_filename: str = ""
    checksum: str = ""
    uploaded_at: datetime | None = None
    deleted_at: datetime | None = None
    # What this file belongs to — a game id, a post id. Opaque here: this service stores
    # bytes and does not model the catalogue.
    reference_id: str = ""

    @classmethod
    def create(
        cls,
        *,
        media_id: str,
        owner_id: str,
        kind: MediaKind,
        declared_type: str,
        sniffed_type: str,
        size_bytes: int,
        filename: str = "",
        checksum: str = "",
        reference_id: str = "",
        visibility: Visibility | None = None,
        now: datetime,
    ) -> MediaObject:
        """Validate an upload and describe where it will be stored."""
        content_type = validate_content(
            kind=kind, declared_type=declared_type, sniffed_type=sniffed_type
        )
        validate_size(kind=kind, size_bytes=size_bytes)

        return cls(
            id=media_id,
            owner_id=owner_id,
            kind=kind,
            # An explicit visibility is honoured only where it makes something *more*
            # restrictive. A caller cannot ask for a public game binary.
            visibility=_resolve_visibility(kind, visibility),
            content_type=content_type,
            size_bytes=size_bytes,
            object_key=object_key_for(media_id),
            original_filename=sanitise_filename(filename),
            checksum=checksum,
            reference_id=reference_id.strip()[:64],
            uploaded_at=now,
        )

    # --- access ----------------------------------------------------------

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    @property
    def is_public(self) -> bool:
        return self.visibility is Visibility.PUBLIC

    def readable_by(self, *, user_id: str, is_staff: bool, has_read_scope: bool) -> bool:
        """Whether this caller may read the bytes.

        A public object is readable by anyone — that is what public means, and a storefront
        image behind a login is an image nobody sees. A private one is readable by its
        owner, by staff, and by a service holding `media:read`, which is how the catalog
        hands a download to a user who has actually bought the game.
        """
        if self.is_deleted:
            return False
        if self.is_public:
            return True
        return bool(user_id) and (user_id == self.owner_id or is_staff or has_read_scope)

    def assert_owner(self, user_id: str, *, is_staff: bool = False) -> None:
        if user_id != self.owner_id and not is_staff:
            raise errors.permission_denied(
                "this file belongs to another user", reason=REASON_NOT_OWNER
            )

    def assert_available(self) -> None:
        if self.is_deleted:
            raise errors.not_found(f"media {self.id} was deleted", reason=REASON_DELETED)

    def delete(self, *, now: datetime) -> bool:
        """Mark it deleted. Returns whether anything changed.

        A soft delete. A catalogue entry or a community post may still reference this id,
        and a hard delete would turn those into broken links with no way to tell what they
        pointed at. The bytes are removed; the record of what existed is not.
        """
        if self.is_deleted:
            return False
        self.deleted_at = now
        return True


# --- validation ----------------------------------------------------------


def _resolve_visibility(kind: MediaKind, requested: Visibility | None) -> Visibility:
    default = DEFAULT_VISIBILITY[kind]
    if requested is None:
        return default
    # A request may tighten but never loosen. Otherwise a client could publish a game
    # build by passing one field.
    if default is Visibility.PRIVATE and requested is Visibility.PUBLIC:
        raise errors.invalid_argument(
            f"a {kind} cannot be made public",
            reason="VISIBILITY_NOT_ALLOWED",
        )
    return requested


def validate_size(*, kind: MediaKind, size_bytes: int) -> None:
    if size_bytes <= 0:
        raise errors.invalid_argument("the uploaded file is empty", reason=REASON_EMPTY)
    limit = MAX_SIZE[kind]
    if size_bytes > limit:
        raise errors.invalid_argument(
            f"a {kind} may be at most {limit // MB} MB; this one is {size_bytes // MB} MB",
            reason=REASON_TOO_LARGE,
            limit_bytes=limit,
            size_bytes=size_bytes,
        )


def validate_content(*, kind: MediaKind, declared_type: str, sniffed_type: str) -> str:
    """Check the type against the allowlist, and against the bytes.

    Both halves matter. The allowlist stops a category of file we never want. The
    comparison with the sniffed type stops the much more common case: a real image
    extension on something that is not an image, or an HTML page announced as a PNG so that
    it is served from our own origin.

    Returns the type to store, which is the **sniffed** one where it is known.
    """
    declared = _normalise_type(declared_type)
    allowed = ALLOWED_TYPES[kind]

    if declared not in allowed:
        raise errors.invalid_argument(
            f"a {kind} may not be {declared or 'of an unknown type'}; allowed: "
            f"{', '.join(sorted(allowed))}",
            reason=REASON_TYPE_NOT_ALLOWED,
            declared_type=declared,
        )

    if not sniffed_type:
        # Nothing recognisable in the header. Accepted only where the kind's allowlist is
        # genuinely open-ended — a game build is an opaque archive — and refused where a
        # real signature is expected.
        if kind in (MediaKind.IMAGE, MediaKind.VIDEO):
            raise errors.invalid_argument(
                "the file does not look like the type it claims to be",
                reason=REASON_TYPE_MISMATCH,
                declared_type=declared,
            )
        return declared

    if sniffed_type != declared:
        # A specific message, because this is the check most likely to fire on an honest
        # mistake — a .jpg that is really a .png — as well as on an attack.
        raise errors.invalid_argument(
            f"this file is a {sniffed_type} but was declared as {declared}",
            reason=REASON_TYPE_MISMATCH,
            declared_type=declared,
            actual_type=sniffed_type,
        )

    if sniffed_type not in allowed:
        raise errors.invalid_argument(
            f"a {kind} may not be {sniffed_type}",
            reason=REASON_TYPE_NOT_ALLOWED,
            actual_type=sniffed_type,
        )

    return sniffed_type


def _normalise_type(value: str) -> str:
    """Strip parameters and case. ``image/PNG; charset=binary`` is ``image/png``."""
    return (value or "").split(";")[0].strip().lower()


def sanitise_filename(filename: str) -> str:
    """Reduce a filename to something safe to echo back.

    It is never used to build a path — the object key comes from the id — so this only has
    to be safe to put in a `Content-Disposition` header and in a log line. Directory
    separators, null bytes and control characters all go.
    """
    if not filename:
        return ""
    cleaned = filename.replace("\\", "/").split("/")[-1]
    cleaned = "".join(ch for ch in cleaned if ch.isprintable() and ch not in '"\\')
    cleaned = cleaned.strip().strip(".")
    if len(cleaned) > FILENAME_MAX:
        cleaned = cleaned[-FILENAME_MAX:]
    return cleaned


def object_key_for(media_id: str) -> str:
    """Where the bytes live, derived only from the id.

    Sharded by the first four characters. One flat directory with a million files is slow
    to list and unpleasant to work with on most filesystems; two levels keeps each
    directory small.

    Nothing from the request reaches this, which is why path traversal is not a filter here
    but an impossibility.
    """
    safe = "".join(ch for ch in media_id if ch.isalnum() or ch in "-_")
    if len(safe) < 4:
        safe = safe.ljust(4, "0")
    return f"{safe[:2]}/{safe[2:4]}/{safe}"
