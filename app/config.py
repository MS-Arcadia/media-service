"""Media service configuration."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, model_validator

from app.platform.config import BaseConfig


class Config(BaseConfig):
    service_name: str = "media-service"
    http_port: int = 8084

    # Where the bytes go. A directory rather than a MinIO bucket — see
    # adapters/outbound/filesystem.py for the trade-off and how to change it.
    storage_root: str = "/var/lib/arcadia/media"

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

    topic_media_events: str = "media-events"

    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])

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


@lru_cache
def get_config() -> Config:
    return Config()  # type: ignore[call-arg]
