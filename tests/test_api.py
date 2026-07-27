"""The HTTP edge, against the real app and a real temporary directory.

The unit tests prove the rules; this proves the wiring — that a multipart upload arrives
intact, that a download streams the same bytes back, and that the response headers that stop
an uploaded file being executed in a browser are actually set.

The database is faked: the unit of work, the metadata repository and the outbox publisher are
in-memory. Everything else is the real thing — FastAPI routing, JWT verification, multipart
parsing, the filesystem object store, the signed URLs, and the response headers.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient

SECRET = "test-only-jwt-secret-at-least-32-characters-long"
DOWNLOAD_SECRET = "test-only-media-download-secret-32-chars"

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 4
ZIP = b"PK\x03\x04" + bytes(range(256)) * 8
HTML = b"<!DOCTYPE html><script>alert(document.cookie)</script>"


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    storage = tmp_path_factory.mktemp("media-store")
    os.environ.update(
        {
            "DATABASE_URL": "postgresql://unused:unused@localhost:5432/unused",
            "JWT_SECRET": SECRET,
            "DOWNLOAD_SECRET": DOWNLOAD_SECRET,
            "STORAGE_ROOT": str(storage),
            "KAFKA_ENABLED": "false",
            "RUN_MIGRATIONS": "false",
            "ENVIRONMENT": "local",
            "LOG_JSON": "false",
            "PUBLIC_BASE_URL": "http://testserver",
        }
    )
    from app.bootstrap import build
    from app.config import get_config

    get_config.cache_clear()
    app = build()

    # Postgres is the one dependency these tests do not provide, so the three collaborators
    # that need it are swapped for the in-memory versions used by the service tests. The
    # object store, the token issuer, the routing and the auth stay real — those are what this
    # file exists to exercise.
    from tests.test_media_service import (
        FakeUnitOfWork,
        InMemoryRepository,
        RecordingPublisher,
    )

    service = app.state.media_service
    service._uow = FakeUnitOfWork()
    service._repo = InMemoryRepository()
    service._publisher = RecordingPublisher()

    return TestClient(app)


@pytest.fixture
def storage_root(client) -> Path:
    return Path(client.app.state.config.storage_root)


def token(*, role: str = "DEVELOPER", user_id: str = "dev-1", scopes: list[str] | None = None):
    return jwt.encode(
        {
            "sub": user_id,
            "role": role,
            "typ": "access",
            "scopes": scopes or [],
            "iss": "arcadia-auth",
            "aud": "arcadia",
            "exp": datetime.now(UTC) + timedelta(minutes=5),
        },
        SECRET,
        algorithm="HS256",
    )


def auth(**kwargs) -> dict[str, str]:
    return {"Authorization": f"Bearer {token(**kwargs)}"}


def upload(client, data: bytes, *, kind: str, content_type: str, name: str = "f.bin", **form):
    return client.post(
        "/v1/media",
        files={"file": (name, data, content_type)},
        data={"kind": kind, **form},
        headers=auth(),
    )


# --- upload --------------------------------------------------------------


def test_a_png_uploads_and_reports_its_metadata(client):
    response = upload(client, PNG, kind="IMAGE", content_type="image/png", name="shot.png")

    assert response.status_code == 201
    body = response.json()
    assert body["content_type"] == "image/png"
    assert body["size_bytes"] == len(PNG)
    assert body["visibility"] == "PUBLIC"
    assert body["filename"] == "shot.png"
    assert body["url"].endswith(f"/v1/media/{body['id']}/content")


def test_the_bytes_actually_reach_the_disk(client, storage_root: Path):
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    stored = list(storage_root.rglob(body["id"]))
    assert len(stored) == 1
    assert stored[0].read_bytes() == PNG


def test_an_html_file_declared_as_a_png_is_rejected(client):
    """Stored and served from our own origin, this would be stored cross-site scripting."""
    response = upload(client, HTML, kind="IMAGE", content_type="image/png", name="evil.png")
    assert response.status_code == 400
    assert response.json()["reason"] == "CONTENT_TYPE_MISMATCH"


def test_an_svg_is_rejected_as_not_on_the_allowlist(client):
    """SVG is XML and can carry script, which is why it is not an allowed image type."""
    svg = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'
    response = upload(client, svg, kind="IMAGE", content_type="image/svg+xml", name="x.svg")
    assert response.status_code == 400


def test_an_empty_upload_is_rejected(client):
    response = upload(client, b"", kind="IMAGE", content_type="image/png")
    assert response.status_code == 400
    assert response.json()["reason"] == "MEDIA_EMPTY"


def test_an_upload_without_a_token_is_rejected(client):
    response = client.post(
        "/v1/media",
        files={"file": ("shot.png", PNG, "image/png")},
        data={"kind": "IMAGE"},
    )
    assert response.status_code == 401


def test_a_game_binary_cannot_be_uploaded_as_public(client):
    response = upload(
        client, ZIP, kind="GAME_BINARY", content_type="application/zip", visibility="PUBLIC"
    )
    assert response.status_code == 400
    assert response.json()["reason"] == "VISIBILITY_NOT_ALLOWED"


# --- download ------------------------------------------------------------


def test_a_public_file_downloads_anonymously_and_intact(client):
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()

    response = client.get(f"/v1/media/{body['id']}/content")

    assert response.status_code == 200
    assert response.content == PNG
    assert response.headers["content-type"] == "image/png"


def test_a_download_is_always_an_attachment(client):
    """`inline` would let an uploaded file execute in the context of our own origin."""
    body = upload(client, PNG, kind="IMAGE", content_type="image/png", name="shot.png").json()
    response = client.get(f"/v1/media/{body['id']}/content")
    assert response.headers["content-disposition"].startswith("attachment;")


def test_a_download_forbids_content_type_sniffing(client):
    """A browser deciding for itself what a file is undoes the type checking at upload."""
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    response = client.get(f"/v1/media/{body['id']}/content")
    assert response.headers["x-content-type-options"] == "nosniff"


def test_a_public_file_is_cacheable_and_a_private_one_is_not(client):
    public = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    private = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()

    public_response = client.get(f"/v1/media/{public['id']}/content")
    assert "immutable" in public_response.headers["cache-control"]

    private_response = client.get(f"/v1/media/{private['id']}/content", headers=auth())
    assert private_response.headers["cache-control"] == "private, no-store"


def test_a_private_file_is_not_downloadable_anonymously(client):
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()
    response = client.get(f"/v1/media/{body['id']}/content")
    # Not found rather than forbidden: "forbidden" confirms the id is real.
    assert response.status_code == 404


def test_a_private_file_downloads_for_its_owner(client):
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()
    response = client.get(f"/v1/media/{body['id']}/content", headers=auth())
    assert response.status_code == 200
    assert response.content == ZIP


def test_a_ticket_downloads_a_private_file_with_no_header(client):
    """The whole point of a signed URL: it works in an <img src> or a download manager."""
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()

    ticket = client.post(f"/v1/media/{body['id']}/ticket", headers=auth()).json()
    path = ticket["url"].replace("http://testserver", "")
    response = client.get(path)

    assert response.status_code == 200
    assert response.content == ZIP
    assert ticket["expires_in_seconds"] == 900


def test_a_tampered_ticket_is_refused(client):
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()
    ticket = client.post(f"/v1/media/{body['id']}/ticket", headers=auth()).json()
    path = ticket["url"].replace("http://testserver", "")[:-4] + "AAAA"

    response = client.get(path)
    assert response.status_code == 403


def test_a_stranger_cannot_get_a_ticket_for_a_private_file(client):
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()
    response = client.post(
        f"/v1/media/{body['id']}/ticket", headers=auth(user_id="stranger", role="BASIC_USER")
    )
    assert response.status_code == 404


def test_a_service_with_the_read_scope_can_download_a_private_file(client):
    """How the catalog hands a build to a user who has bought the game."""
    body = upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip").json()
    response = client.get(
        f"/v1/media/{body['id']}/content",
        headers=auth(user_id="catalog-service", role="SUPPORT", scopes=["media:read"]),
    )
    assert response.status_code == 200


# --- listing and deletion ------------------------------------------------


def test_listing_by_reference_returns_only_public_files(client):
    """A storefront page fetching a game's screenshots must not see the build."""
    upload(client, PNG, kind="IMAGE", content_type="image/png", reference_id="game-99")
    upload(client, ZIP, kind="GAME_BINARY", content_type="application/zip", reference_id="game-99")

    items = client.get("/v1/media/by-reference/game-99").json()

    assert len(items) == 1
    assert items[0]["kind"] == "IMAGE"


def test_deleting_removes_the_bytes(client, storage_root: Path):
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    assert list(storage_root.rglob(body["id"]))

    response = client.delete(f"/v1/media/{body['id']}", headers=auth())

    assert response.status_code == 204
    assert list(storage_root.rglob(body["id"])) == []


def test_a_deleted_file_no_longer_downloads(client):
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    client.delete(f"/v1/media/{body['id']}", headers=auth())
    assert client.get(f"/v1/media/{body['id']}/content").status_code == 404


def test_a_stranger_cannot_delete_someone_elses_file(client):
    body = upload(client, PNG, kind="IMAGE", content_type="image/png").json()
    response = client.delete(
        f"/v1/media/{body['id']}", headers=auth(user_id="stranger", role="BASIC_USER")
    )
    assert response.status_code == 403


# --- operational ---------------------------------------------------------


def test_liveness_needs_no_token(client):
    assert client.get("/livez").json()["status"] == "UP"


def test_readiness_reports_the_storage_check(client):
    """Postgres is deliberately unreachable here, so the overall status is DOWN.

    What this asserts is that the storage check is wired and reports independently — a probe
    that only ever said "UP" would be worth nothing.
    """
    body = client.get("/readyz").json()
    assert body["status"] == "DOWN"
    assert body["checks"]["storage"]["status"] == "UP"
    assert body["checks"]["postgres"]["status"] == "DOWN"
