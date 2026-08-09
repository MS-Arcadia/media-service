"""Media service configuration."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import model_validator

from app.platform.config import BaseConfig


class Config(BaseConfig):
    service_name: str = "media-service"
    http_port: int = 8084

    # Which object store to use. "s3" for MinIO or AWS, "filesystem" for a directory.
    #
    # Defaults to the filesystem so a bare `pytest` and a one-off container need nothing running
    # alongside them. The compose stack sets "s3", because that is the deployment where being
    # stateless is worth another container.
    storage_backend: Literal["filesystem", "s3"] = "filesystem"

    # Where the bytes go with the filesystem backend. Ignored by "s3".
    storage_root: str = "/var/lib/arcadia/media"

    # S3 / MinIO. Required when storage_backend is "s3"; see the validator below.
    s3_endpoint_url: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "arcadia-media"
    s3_region: str = "us-east-1"
    # The buffer that decides single-PUT versus multipart, and therefore the peak memory of one
    # concurrent upload. S3's own minimum for a non-final part is 5 MiB and the adapter refuses
    # anything below it.
    s3_part_size_bytes: int = 8 * 1024 * 1024
    # Create the bucket at boot if it is missing. True locally, where nothing else will; a real
    # deployment usually has it made in advance with a lifecycle policy attached, and should not
    # hand a running service the permission to create buckets.
    s3_create_bucket: bool = True

    # Signs download URLs. A separate secret from JWT_SECRET on purpose: a download token is
    # not an identity token, and one leaking must not compromise the other.
    download_secret: str = ""
    download_ttl_seconds: int = 900

    # Must be reachable by whoever follows the URLs this service hands out — a browser, not
    # the internal Docker network.
    public_base_url: str = "http://localhost:8084"

    # Refuse to accept uploads once the store is this full, so the disk is never filled to
    # the point where Postgres on the same host cannot write either.
    storage_soft_limit_bytes: int = 20 * 1024 * 1024 * 1024

    # And a limit per owner, because the global one alone does not protect anybody: one
    # developer uploading builds in a loop reaches it on their own, and every other developer
    # on the platform is then unable to publish. A fair share is the point, not a total.
    #
    # 5 GiB is roughly two large game builds plus their screenshots. Generous for a real
    # developer, and the wrong order of magnitude for a script.
    owner_quota_bytes: int = 5 * 1024 * 1024 * 1024

    topic_media_events: str = "media-events"

    @property
    def owned_topics(self) -> list[str]:
        return [self.topic_media_events]

    @model_validator(mode="after")
    def _check_download_secret(self) -> Config:
        if not self.download_secret:
            raise ValueError("DOWNLOAD_SECRET is required: it signs download URLs")
        if len(self.download_secret) < 32:
            raise ValueError("DOWNLOAD_SECRET must be at least 32 characters")
        if self.download_secret == self.jwt_secret:
            # Sharing them means a leaked download URL is also a signing oracle for identity
            # tokens.
            raise ValueError("DOWNLOAD_SECRET must differ from JWT_SECRET")
        if self.is_production and "change-me" in self.download_secret.lower():
            raise ValueError("DOWNLOAD_SECRET still holds its development placeholder")
        return self

    @model_validator(mode="after")
    def _check_storage(self) -> Config:
        """Refuse to boot on an S3 configuration that cannot work.

        At boot rather than on first use. A missing endpoint or key surfaces otherwise as a
        failed upload — after a developer has transferred a build — and the error is a botocore
        message about credentials rather than a sentence naming the variable nobody set.
        """
        if self.storage_backend != "s3":
            return self

        missing = [
            name
            for name, value in (
                ("S3_ENDPOINT_URL", self.s3_endpoint_url),
                ("S3_ACCESS_KEY", self.s3_access_key),
                ("S3_SECRET_KEY", self.s3_secret_key),
                ("S3_BUCKET", self.s3_bucket),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"STORAGE_BACKEND=s3 requires {', '.join(missing)}",
            )
        if not self.s3_endpoint_url.startswith(("http://", "https://")):
            raise ValueError("S3_ENDPOINT_URL must include a scheme, e.g. http://minio:9000")
        if self.is_production and self.s3_endpoint_url.startswith("http://"):
            # Credentials are signed rather than sent, but the objects themselves are not, and a
            # game build crossing a network in plaintext is worth refusing outright.
            raise ValueError("S3_ENDPOINT_URL must be https outside development")
        if self.is_production and self.s3_create_bucket:
            # A service that can create buckets can create them by accident — a typo in
            # S3_BUCKET silently starts a fresh, empty store rather than failing.
            raise ValueError("S3_CREATE_BUCKET must be false outside development")
        return self


@lru_cache
def get_config() -> Config:
    return Config()  # type: ignore[call-arg]
