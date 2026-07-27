"""Configuration loading, from the environment.

Every value here is set as a real environment variable, because that is the only thing that
exercises the code path which failed: pydantic-settings' `EnvSettingsSource` decodes
complex-typed fields *before* validators run, and passing the same value as a keyword
argument skips that entirely.

The service passed every unit test and then crash-looped in Docker for exactly this reason.
A test that constructs `Config(kafka_brokers="kafka:9092")` still passes with the bug
present — so these use `monkeypatch.setenv` throughout.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import Config

BASE_ENV = {
    "DATABASE_URL": "postgresql://u:p@postgres:5432/arcadia_media",
    "JWT_SECRET": "a-test-only-jwt-secret-at-least-32-chars",
    "DOWNLOAD_SECRET": "a-test-only-download-secret-32-chars-x",
    # Suppressed deliberately: this path is never written to. Loading the config does not
    # touch the filesystem, and the tests that do use pytest's tmp_path.
    "STORAGE_ROOT": "/tmp/arcadia-media-test",  # noqa: S108
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch):
    """A clean environment holding only what we set.

    Any ARCADIA-relevant variable inherited from the shell would make these tests pass or
    fail depending on who ran them.
    """
    for key in [
        *BASE_ENV,
        "KAFKA_BROKERS",
        "CORS_ORIGINS",
        "KAFKA_ENABLED",
        "HTTP_PORT",
        "ENVIRONMENT",
        "JWT_ALGORITHM",
        "JWT_PUBLIC_KEY",
        "CURRENCY",
    ]:
        monkeypatch.delenv(key, raising=False)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    # model_config is a dict, so setitem rather than setattr. The repo ships a .env.example
    # and not a .env, but a developer with a local .env would otherwise leak it into these
    # assertions and make them pass or fail depending on whose machine ran them.
    monkeypatch.setitem(Config.model_config, "env_file", None)
    return monkeypatch


def load(env: pytest.MonkeyPatch, **environ: str) -> Config:
    for key, value in environ.items():
        env.setenv(key, value)
    return Config()  # type: ignore[call-arg]


# --- the boot failure this file exists for -------------------------------


def test_kafka_brokers_accepts_a_bare_hostport(env):
    """The regression. `KAFKA_BROKERS=kafka:9092` is what the compose file sets.

    Without ``NoDecode`` on the field, pydantic-settings tried to JSON-decode this first and
    raised "error parsing value for field kafka_brokers" — so the container crash-looped
    after every unit test had passed.
    """
    cfg = load(env, KAFKA_BROKERS="kafka:9092")
    assert cfg.kafka_brokers == ["kafka:9092"]


def test_kafka_brokers_accepts_a_comma_separated_list(env):
    cfg = load(env, KAFKA_BROKERS="kafka-1:9092,kafka-2:9092, kafka-3:9092")
    assert cfg.kafka_brokers == ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]


def test_a_single_broker_is_not_split_into_characters(env):
    cfg = load(env, KAFKA_BROKERS="localhost:9092")
    assert cfg.kafka_brokers == ["localhost:9092"]


def test_cors_origins_takes_the_same_form(env):
    cfg = load(env, CORS_ORIGINS="http://localhost:3000,https://arcadia.example")
    assert cfg.cors_origins == ["http://localhost:3000", "https://arcadia.example"]


def test_an_empty_list_variable_is_empty_not_a_blank_entry(env):
    """`CORS_ORIGINS=` in a .env file is a real thing people write."""
    cfg = load(env, CORS_ORIGINS="")
    assert cfg.cors_origins == []


def test_the_default_broker_applies_when_the_variable_is_absent(env):
    cfg = load(env)
    assert cfg.kafka_brokers == ["localhost:9092"]


def test_a_json_array_is_also_accepted(env):
    """Not required, but somebody will write it, and refusing it would be gratuitous."""
    cfg = load(env, KAFKA_BROKERS='["a:9092","b:9092"]')
    assert len(cfg.kafka_brokers) == 2


# --- validation at boot --------------------------------------------------


def test_a_missing_database_url_fails_at_boot(env):
    env.delenv("DATABASE_URL")
    with pytest.raises(ValidationError):
        Config()  # type: ignore[call-arg]


def test_a_short_jwt_secret_is_refused(env):
    """A 32-character minimum, checked at boot rather than on the first request."""
    with pytest.raises(ValidationError, match="at least 32"):
        load(env, JWT_SECRET="too-short")


def test_a_development_placeholder_is_refused_in_production(env):
    with pytest.raises(ValidationError, match="placeholder"):
        load(
            env,
            ENVIRONMENT="production",
            JWT_SECRET="local-development-jwt-secret-change-me-x",
        )


def test_the_same_placeholder_is_fine_locally(env):
    cfg = load(env, ENVIRONMENT="local", JWT_SECRET="local-development-jwt-secret-change-me-x")
    assert cfg.is_production is False


def test_an_unknown_environment_is_refused(env):
    with pytest.raises(ValidationError):
        load(env, ENVIRONMENT="staging-2")


def test_an_unsupported_jwt_algorithm_is_refused(env):
    with pytest.raises(ValidationError, match="unsupported"):
        load(env, JWT_ALGORITHM="ES256")


def test_rs256_requires_a_public_key_not_a_secret(env):
    with pytest.raises(ValidationError, match="JWT_PUBLIC_KEY"):
        load(env, JWT_ALGORITHM="RS256")


# --- other types from their string form ----------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"), [("false", False), ("true", True), ("0", False), ("1", True)]
)
def test_booleans_are_read_from_their_string_form(env, raw: str, expected: bool):
    """`KAFKA_ENABLED=false` in the compose file has to mean False, not a truthy string."""
    assert load(env, KAFKA_ENABLED=raw).kafka_enabled is expected


def test_ports_are_read_as_integers(env):
    assert load(env, HTTP_PORT="8084").http_port == 8084


def test_a_postgres_scheme_dsn_is_accepted(env):
    """The compose file hands out `postgresql://`, matching the Go services.

    Rewriting it for asyncpg happens in platform.db rather than by demanding a second
    spelling in the environment.
    """
    cfg = load(env, DATABASE_URL="postgres://u:p@postgres:5432/arcadia_media")
    assert cfg.database_url.startswith("postgres")


# --- the token contract shared with the Go services ----------------------


def test_the_issuer_and_audience_default_to_what_the_go_services_require(env):
    """wallet-service and payment-service default these to "arcadia-auth" and "arcadia".

    They therefore *require* the iss and aud claims. Leaving them empty here meant a token
    the Python services accepted was rejected by the Go ones — and, worse, that a token from
    a completely different issuer would have been accepted by three of the five services.

    The weakest verifier defines the platform's security, so these align upward. Changing
    either default is a change to a cross-language contract, which is what this test is for.
    """
    cfg = load(env)
    assert cfg.jwt_issuer == "arcadia-auth"
    assert cfg.jwt_audience == "arcadia"


def test_the_checks_can_be_turned_off_deliberately(env):
    """Empty means "do not check", which has to remain possible — but as an explicit act
    rather than as the default."""
    cfg = load(env, JWT_ISSUER="", JWT_AUDIENCE="")
    assert cfg.jwt_issuer == ""
    assert cfg.jwt_audience == ""
