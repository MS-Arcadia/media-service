-- Media metadata and the outbox.
--
-- This service stores bytes and the minimum needed to find them again. What a file *means* —
-- which game it illustrates, which post it belongs to — is another service's business and is
-- referenced here only as an opaque id.

CREATE TABLE IF NOT EXISTS media_objects (
    id                TEXT        PRIMARY KEY,
    owner_id          TEXT        NOT NULL,
    kind              TEXT        NOT NULL,
    visibility        TEXT        NOT NULL,
    content_type      TEXT        NOT NULL,
    size_bytes        BIGINT      NOT NULL,

    -- Derived from the id, never from the uploaded filename. That is what makes path
    -- traversal structurally impossible rather than merely filtered.
    object_key        TEXT        NOT NULL UNIQUE,

    -- Kept only to offer back on download. Never used to build a path.
    original_filename TEXT        NOT NULL DEFAULT '',
    checksum          TEXT        NOT NULL DEFAULT '',

    -- The game or post this belongs to. Opaque: no foreign key, because the referenced row
    -- lives in another service's database.
    reference_id      TEXT        NOT NULL DEFAULT '',

    uploaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Soft delete. A catalogue entry may still reference this id, and a hard delete would
    -- turn that into a dangling reference with nothing to explain it.
    deleted_at        TIMESTAMPTZ,

    CONSTRAINT media_kind_known CHECK (kind IN ('IMAGE','VIDEO','FILE','GAME_BINARY')),
    CONSTRAINT media_visibility_known CHECK (visibility IN ('PUBLIC','PRIVATE')),

    -- An empty file is never a legitimate upload.
    CONSTRAINT media_size_positive CHECK (size_bytes > 0),

    -- The single most consequential rule in this service, restated where nothing can bypass
    -- it: a game build is never public. An unauthenticated URL for one is a pirated copy.
    CONSTRAINT media_game_binary_is_private CHECK (
        kind <> 'GAME_BINARY' OR visibility = 'PRIVATE'
    )
);

CREATE INDEX IF NOT EXISTS ix_media_owner_id   ON media_objects (owner_id);
CREATE INDEX IF NOT EXISTS ix_media_owner_kind ON media_objects (owner_id, kind);
-- How a storefront page fetches a game's screenshots.
CREATE INDEX IF NOT EXISTS ix_media_reference  ON media_objects (reference_id)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS outbox_messages (
    id            BIGSERIAL   PRIMARY KEY,
    event_id      TEXT        NOT NULL UNIQUE,
    event_type    TEXT        NOT NULL,
    topic         TEXT        NOT NULL,
    partition_key TEXT        NOT NULL,
    envelope      JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    attempts      INTEGER     NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE INDEX IF NOT EXISTS ix_outbox_pending
    ON outbox_messages (id)
    WHERE published_at IS NULL;
