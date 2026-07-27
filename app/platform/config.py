"""Configuration common to every Arcadia Python service.

Two properties are deliberate. Validation happens at boot, so a misconfigured service
refuses to start rather than failing on the first request that needs the missing value.
And unsafe defaults are refused in production: a development JWT secret that silently
works in production is worse than no default at all.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

# A list that arrives as a comma-separated environment variable.
#
# NoDecode is essential, not decoration. Without it pydantic-settings tries to JSON-decode
# any complex-typed field before validators run, so KAFKA_BROKERS=kafka:9092 fails at boot
# with "error parsing value" — it is not valid JSON. NoDecode hands the raw string to the
# validator below, which splits it.
CsvList = Annotated[list[str], NoDecode]


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- service ---------------------------------------------------------
    service_name: str = "service"
    service_version: str = "dev"
    environment: str = Field(default="local")
    log_level: str = "INFO"
    log_json: bool = True

    # --- http ------------------------------------------------------------
    # Binding all interfaces is required inside a container: the port is published by
    # the runtime, and 127.0.0.1 would be unreachable from outside the container.
    http_host: str = "0.0.0.0"  # noqa: S104
    http_port: int = 8080
    cors_origins: CsvList = Field(default_factory=list)
    request_timeout_seconds: float = 15.0

    # --- database --------------------------------------------------------
    database_url: str
    db_pool_size: int = 5
    db_max_overflow: int = 5
    db_echo: bool = False
    run_migrations: bool = True

    # --- kafka -----------------------------------------------------------
    kafka_enabled: bool = True
    kafka_brokers: CsvList = Field(default_factory=lambda: ["localhost:9092"])
    kafka_ensure_topics: bool = True
    kafka_topic_partitions: int = 1
    kafka_topic_replication: int = 1
    outbox_interval_seconds: float = 1.0
    outbox_batch_size: int = 100

    # --- auth ------------------------------------------------------------
    jwt_secret: str = ""
    jwt_public_key: str = ""
    jwt_algorithm: str = "HS256"
    # Matched to the Go services' defaults on purpose.
    #
    # wallet-service and payment-service default these to "arcadia-auth" and "arcadia" and
    # therefore *require* the iss and aud claims. Leaving them empty here meant three services
    # accepted a token these two rejected — and, worse, would have accepted a token minted by
    # a different issuer altogether. The weakest verifier defines the platform's security, so
    # these align upward rather than down.
    #
    # Set either to "" to stop checking it, which is a deliberate act rather than a default.
    jwt_issuer: str = "arcadia-auth"
    jwt_audience: str = "arcadia"

    @field_validator("kafka_brokers", "cors_origins", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        """Accept a comma-separated string, which is what an env var actually is.

        Paired with ``NoDecode`` on the field: that stops pydantic-settings trying to parse
        the value as JSON first, and this turns the raw string into a list.
        """
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("environment")
    @classmethod
    def _known_environment(cls, value: str) -> str:
        allowed = {"local", "development", "staging", "production"}
        lowered = value.lower()
        if lowered not in allowed:
            raise ValueError(f"environment must be one of {sorted(allowed)}, got {value!r}")
        return lowered

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @model_validator(mode="after")
    def _check_secrets(self) -> BaseConfig:
        if self.jwt_algorithm.startswith("HS"):
            if not self.jwt_secret:
                raise ValueError("JWT_SECRET is required with an HS* algorithm")
            if len(self.jwt_secret) < 32:
                raise ValueError("JWT_SECRET must be at least 32 characters")
            if self.is_production and "change-me" in self.jwt_secret.lower():
                raise ValueError("JWT_SECRET still holds its development placeholder")
        elif self.jwt_algorithm.startswith("RS"):
            if not self.jwt_public_key:
                raise ValueError("JWT_PUBLIC_KEY is required with an RS* algorithm")
        else:
            raise ValueError(f"unsupported JWT_ALGORITHM {self.jwt_algorithm!r}")

        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        return self
