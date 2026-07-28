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

### Two object stores, chosen by one variable

`STORAGE_BACKEND` is `s3` (MinIO, as the architecture document specifies) or `filesystem` (a
directory). The compose stack sets `s3`; the service defaults to `filesystem` so a bare `pytest`
and a one-off container need nothing running alongside them.

Both exist on purpose, because they trade opposite things:

| | filesystem | s3 |
|---|---|---|
| Extra containers | none | MinIO, ~512 MB |
| Replicas | **one** — two do not see each other's files | any |
| Store outgrows one disk | it cannot | it can |
| Shares a disk with Postgres | yes, and filling it stops the database writing | no |
| Presigned download offload | not possible | possible later |

The second one is why S3 exists: a filesystem store makes this service **stateful**, and being
stateless is the precondition for running more than one of it.

**The port was drawn in the right place, and there is now evidence for that rather than a
claim.** This README used to say an S3 adapter would be "a new file beside it and one line in
`bootstrap.py`". It was:
[`adapters/outbound/s3.py`](app/adapters/outbound/s3.py) plus one `if` choosing the backend.
Nothing in the domain, the use cases or the REST edge changed. Two things did move into the port
that had been assumed filesystem-shaped — `check_ready` and `start`/`aclose` — because "is this
store usable" has a different answer per backend and only the backend knows it.

Each adapter then behaves properly *for its own medium*. The filesystem streams to a temporary
file, `fsync`s it and does an atomic `os.replace`, with every blocking call in a worker thread.
S3 buffers one part, sends a single `PUT` if the file fits inside it — every screenshot does —
and escalates to multipart only for something genuinely large, never holding more than one part
in memory. Both compute their own sha256 as the bytes pass through, because S3's `ETag` is an MD5
for one PUT and something else entirely for a multipart upload.

**Switching an existing store needs a copy.** The metadata rows survive a backend change and
their bytes do not, so every download 404s until the objects are moved:

```bash
make -C infra media-migrate     # volume -> bucket, safe to re-run
```

Going back the other way is the same `mc mirror` reversed. `make e2e` catches a store that was
switched without one — `test_no_media_row_lacks_its_bytes` reads whichever backend is running
and lists the rows whose objects are missing.

### Nothing ever holds a whole file

Uploads and downloads both stream in 1 MiB chunks. A 4 GB build read into memory would take a
128 MB container down long before a byte reached disk — and would do it once per concurrent
request.

That constrains the order of the checks. The **type** is decided from the first chunk and
validated before anything is written, so a rejected upload costs one buffer rather than a whole
file on disk. The **size** cannot be known in advance from anything trustworthy — `Content-Length`
is client-supplied — so the store enforces it *while writing* and aborts mid-stream. Waiting
until the end would mean putting the whole oversized file on disk before rejecting it, which is
exactly the resource exhaustion the limit exists to prevent.

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
│       ├── filesystem.py   ─┬─ the two ObjectStore implementations ─
│       ├── s3.py           ─┘  (STORAGE_BACKEND picks one)
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
| `test_s3_store.py` | The S3 adapter: multipart escalation against a fake, round trips against a real MinIO |

`test_s3_store.py` is in two halves. The fake-client half asserts how many requests a small file
costs — a real server answers correctly either way and says nothing about the round trips. The
other half needs a live MinIO and skips without one:

```bash
ARCADIA_S3_ENDPOINT=http://localhost:9000 pytest tests/test_s3_store.py
```

That half is not optional thoroughness. This adapter shipped with a download that streamed
**nothing** behind a declared `Content-Length` — `async with response["Body"] as body` binds the
underlying aiohttp response, which has no `iter_chunks` — and every fake passed. The first real
read caught it.
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
