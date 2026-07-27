# Arcadia — Media Service

The platform's only file store: teasers, screenshots, post attachments and game builds for
[Arcadia](../PHASE01/README.md).

Python 3.12, FastAPI, clean architecture, PostgreSQL, Kafka.

---

## Quick start

```bash
make install
make test
make docker
```

The tests include a full HTTP round trip — real multipart upload, real bytes on disk, real
signed-URL download — against a temporary directory. Only Postgres is faked.

```bash
cd ../infra && make images && make up && make wait
curl -s localhost:8084/readyz
```

Interactive API docs at http://localhost:8084/docs.

---

## What it does

The Media part of requirement 1.8, plus the file storage the catalog and community services
depend on.

| Capability | Notes |
|---|---|
| Upload | Type checked against the file's **actual bytes**, size limited per kind |
| Serve public assets | Screenshots and teasers, cacheable, no login needed |
| Serve private files | Through short-lived signed URLs |
| List by reference | Every public file attached to one game or post |
| Delete | Bytes removed, record kept |

---

## The decisions worth explaining

### The declared content type is worthless on its own

`Content-Type` is supplied by whoever is uploading. So every upload's first bytes are checked
against a small allowlist of real magic numbers, and a declared type that disagrees with the
bytes is refused.

The attack this stops: an HTML page uploaded as `shot.png`. Served from our own origin it
becomes stored cross-site scripting. SVG is refused outright for the same reason — it is XML
and can carry script, so it is not on the image allowlist at all.

Two more layers behind it, because a browser that guesses undoes the check:

* `Content-Disposition: attachment`, always. Never `inline`.
* `X-Content-Type-Options: nosniff`.

### A game build can never be public

`PUBLIC` is for storefront assets — a screenshot behind a login is a screenshot nobody sees.
`PRIVATE` is for anything paid for or unreleased.

The default comes from the **kind**, not the request, and a request may only make an object
*more* restrictive. There is no field a client can set to publish a build. The same rule is
restated as a `CHECK` constraint:

```sql
CONSTRAINT media_game_binary_is_private CHECK (
    kind <> 'GAME_BINARY' OR visibility = 'PRIVATE'
)
```

An unauthenticated URL for a build is a pirated copy, so it is worth saying twice.

### Signed download URLs, not bearer tokens

A download usually is not made by an API client. It goes into an `<img src>`, a `<video>`, or
a download manager — none of which attaches an `Authorization` header.

So `POST /v1/media/{id}/ticket` authorises **once** and returns a URL that carries its own
proof: HMAC-SHA256 signed, expiring in fifteen minutes, and bound to one media id. A ticket
for a screenshot cannot fetch a build. The signature is compared with `hmac.compare_digest`,
because a byte-by-byte `==` leaks how much of a forged signature was right.

This is the local equivalent of an S3 presigned URL, and it is signed with a **separate**
secret from `JWT_SECRET`. Boot fails if they match: a download token is not an identity
token, and one leaking must not compromise the other.

### An object key is derived from the id, never from the filename

```
media id  "abcd1234"  →  key  "ab/cd/abcd1234"
```

There is no code path in which user input reaches the filesystem as a path, which makes path
traversal structurally impossible rather than filtered. The uploaded filename is kept only to
offer back on download, stripped of separators, quotes and control characters.

The store still refuses a key that resolves outside its root. That cannot trigger with
today's callers — which is exactly why it raises `INTERNAL` rather than a client error.

### Bytes are written before the metadata row is committed

Both orders can fail. This one leaves an orphaned file: wasted space, cleared by a sweep. The
other leaves a metadata row pointing at a file that does not exist — a broken image in a
storefront and a 404 for something the catalogue says exists.

Wasted bytes are cheaper than a lie. A failed commit deletes the bytes it just wrote, so the
common failure leaks nothing at all; only a crash in between leaves an orphan, and the
boot-time sweep clears the partial writes.

### Deletion is soft

A catalogue entry or a community post may still reference the id. The bytes go; the record
that they existed does not, so a dangling reference can still say what it pointed at.

### The filesystem instead of MinIO

The architecture document specifies MinIO. This runs on a directory, because MinIO is another
container, another ~300 MB of memory and another credential — and the platform runs on plain
Docker with a stated shortage of resources.

**The trade-off is real.** A filesystem store means this service is *not* stateless: two
replicas do not see each other's files without a shared volume, and there is no replication.
That is fine for a single-replica local and demo deployment and is not fine in production.

It is also cheap to change. Everything above
[`adapters/outbound/filesystem.py`](app/adapters/outbound/filesystem.py) talks to the
`ObjectStore` protocol, so an S3 or MinIO adapter is a new file beside it and one line in
`bootstrap.py`. Nothing in the domain or the use cases moves.

Within that choice, the adapter behaves properly: writes go to a temporary file and are
`fsync`ed before an atomic `os.replace`, all file I/O runs in a worker thread so a 4 GB
upload does not stall the event loop, and downloads stream in 1 MiB chunks rather than
reading a build into memory.

---

## Architecture

```
app/
├── domain/
│   ├── media.py            the MediaObject aggregate: limits, allowlist, visibility
│   └── content.py          magic-number identification, pure and dependency-free
├── application/
│   ├── ports.py            the interfaces, including ObjectStore
│   ├── media_service.py    the use cases
│   └── download_token.py   the signed URLs
├── adapters/
│   ├── inbound/rest/       FastAPI routers
│   └── outbound/
│       ├── filesystem.py   ── the ObjectStore implementation ──
│       ├── repositories.py PostgreSQL metadata
│       └── publisher.py    the transactional outbox
├── platform/               general-purpose plumbing, vendored — see the catalog README
├── config.py
└── bootstrap.py
migrations/
tests/
```

---

## Limits

| Kind | Max size | Allowed types | Default visibility |
|---|---|---|---|
| `IMAGE` | 10 MB | png, jpeg, webp, gif | PUBLIC |
| `VIDEO` | 200 MB | mp4, webm | PUBLIC |
| `FILE` | 50 MB | pdf, text/plain, zip | PRIVATE |
| `GAME_BINARY` | 4 GB | zip, 7z, octet-stream | PRIVATE (enforced) |

`GAME_BINARY` allows an unrecognised header, because a build genuinely is an opaque archive.
An `IMAGE` or `VIDEO` with no recognisable signature is refused: a real one always has a
magic number.

---

## API

| | |
|---|---|
| `POST /v1/media` | Upload. Multipart: `file`, `kind`, optional `reference_id`, `visibility`. |
| `GET /v1/media` | Your own files |
| `GET /v1/media/{id}` | Metadata |
| `POST /v1/media/{id}/ticket` | A short-lived signed download URL |
| `GET /v1/media/{id}/content` | The bytes. Public, or with `?token=`, or as owner/staff. |
| `GET /v1/media/by-reference/{id}` | Public files attached to a game or post |
| `DELETE /v1/media/{id}` | Soft delete |

Plus `/livez`, `/readyz` and `/metrics`.

An object the caller may not read is reported as **404, not 403**. "Forbidden" confirms the id
is real, which tells somebody enumerating ids that they have found an unreleased build.

### Events

Published on `media-events`: `MediaUploaded`, `MediaDeleted`.

This service consumes nothing. It is a leaf: the catalog and community services call it and
store the ids it returns.

---

## Configuration

| | Required | |
|---|---|---|
| `DATABASE_URL` | yes | PostgreSQL DSN |
| `JWT_SECRET` | yes | Must match Auth. At least 32 characters. |
| `DOWNLOAD_SECRET` | yes | Signs download URLs. **Must differ from `JWT_SECRET`.** |
| `STORAGE_ROOT` | | Default `/var/lib/arcadia/media` |
| `PUBLIC_BASE_URL` | | Default `http://localhost:8084` |
| `DOWNLOAD_TTL_SECONDS` | | Default `900` |
| `HTTP_PORT` | | Default `8084` |

`PUBLIC_BASE_URL` is the one that catches people out: it must be reachable by whoever follows
the URLs — a browser — not a name that only resolves on the internal Docker network.

---

## Testing

```bash
make test
```

| File | What it covers |
|---|---|
| `test_upload_rules.py` | The allowlist, the size limits, visibility, filename sanitising |
| `test_download_tokens.py` | Adversarial: tampering, expiry extension, wrong-file tokens |
| `test_filesystem_store.py` | The real adapter — atomicity, sharding, chunked streaming |
| `test_media_service.py` | The use cases, and the bytes-before-metadata ordering |
| `test_api.py` | A full HTTP round trip and the security response headers |

The ones worth reading first:

* `test_an_html_file_declared_as_a_png_is_refused` — the attack the sniffer exists for
* `test_a_game_binary_cannot_be_made_public`
* `test_a_token_for_one_file_cannot_fetch_another`
* `test_a_failed_commit_removes_the_orphaned_bytes`
* `test_a_key_escaping_the_root_is_refused`

---

## Operational notes

**`/readyz` treats storage as critical**, and checks it by writing a byte rather than by
looking for a directory. A read-only mount and a missing volume both look fine to a directory
check and fail on the first upload — which is exactly the failure a readiness probe should
catch first.

**Watch `arcadia_media_stored_bytes`.** This service is the one whose disk fills. When it does,
Postgres on the same host stops being able to write either, so the ceiling matters more than
it looks.

**Orphaned files** are the expected residue of a crash mid-upload. Partial writes are cleared
at boot; a fully written file whose row never committed is not, and needs a reconciliation
sweep comparing the store against `media_objects`. That sweep is not implemented — the
orphans are harmless and rare, and it is the honest first thing to add if the store grows.

**A `MEDIA_BYTES_MISSING` error** means a row exists and its bytes do not: an interrupted
delete, or something outside this service touching the store. It is logged at ERROR with the
object key.
