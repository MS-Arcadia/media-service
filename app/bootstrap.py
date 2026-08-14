"""The only place that chooses concrete infrastructure.

Notably: this is where ``ObjectStore`` becomes a directory on disk. Swapping in MinIO or S3
is a different class on one line here.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from prometheus_client import Gauge
from sqlalchemy import text

from app.adapters.inbound.rest import media as media_routes
from app.adapters.outbound.filesystem import FilesystemObjectStore
from app.adapters.outbound.publisher import OutboxPublisher
from app.adapters.outbound.repositories import PostgresMediaRepository
from app.adapters.outbound.s3 import S3ObjectStore
from app.application.download_token import TokenIssuer
from app.application.media_service import MediaService
from app.application.ports import ObjectStore
from app.config import Config, get_config
from app.platform import health, kafka, migrate
from app.platform import logging as logx
from app.platform.auth import Verifier
from app.platform.db import UnitOfWork, create_engine, create_session_factory, strip_asyncpg_dsn
from app.platform.events import new_id
from app.platform.http import (
    install_error_handlers,
    install_middleware,
    install_operational_routes,
)
from app.platform.outbox import Dispatcher

logger = logging.getLogger(__name__)

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"

stored_bytes = Gauge(
    "arcadia_media_stored_bytes",
    "Total size of stored media, from the metadata table.",
)


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def build(config: Config | None = None) -> FastAPI:
    cfg = config or get_config()

    logx.configure(
        service=cfg.service_name,
        version=cfg.service_version,
        level=cfg.log_level,
        json_format=cfg.log_json,
    )

    engine = create_engine(
        cfg.database_url,
        pool_size=cfg.db_pool_size,
        max_overflow=cfg.db_max_overflow,
        echo=cfg.db_echo,
    )
    sessions = create_session_factory(engine)
    uow = UnitOfWork(sessions)

    repository = PostgresMediaRepository()
    # The one branch the second backend cost. Everything below this line — the use cases, the
    # REST edge, the readiness probe, the metrics — is written against `ObjectStore` and does not
    # know which of the two it got.
    store: ObjectStore
    if cfg.storage_backend == "s3":
        store = S3ObjectStore(
            endpoint_url=cfg.s3_endpoint_url,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            bucket=cfg.s3_bucket,
            region=cfg.s3_region,
            part_size=cfg.s3_part_size_bytes,
            create_bucket=cfg.s3_create_bucket,
        )
    else:
        store = FilesystemObjectStore(cfg.storage_root)
    publisher = OutboxPublisher(
        producer_name=cfg.service_name, media_events_topic=cfg.topic_media_events
    )
    tokens = TokenIssuer(cfg.download_secret, ttl=timedelta(seconds=cfg.download_ttl_seconds))

    media_service = MediaService(
        uow=uow,
        repository=repository,
        store=store,
        publisher=publisher,
        tokens=tokens,
        clock=SystemClock(),
        new_id=new_id,
        public_base_url=cfg.public_base_url,
        s3_public_base_url=cfg.s3_public_base_url,
        s3_bucket=cfg.s3_bucket,
        storage_soft_limit_bytes=cfg.storage_soft_limit_bytes,
        owner_quota_bytes=cfg.owner_quota_bytes,
    )

    producer = kafka.Producer(cfg.kafka_brokers, cfg.service_name) if cfg.kafka_enabled else None
    dispatcher = (
        Dispatcher(
            sessions,
            producer,
            interval=cfg.outbox_interval_seconds,
            batch_size=cfg.outbox_batch_size,
        )
        if producer is not None
        else None
    )

    probes = health.Registry(service=cfg.service_name, version=cfg.service_version)

    async def check_database() -> None:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))

    probes.add("postgres", check_database, critical=True)

    # Asked of the store rather than worked out here. "Is this store usable" has a different
    # answer per backend — a writable directory, a writable bucket — and only the backend knows
    # which question to ask. It was inlined for the filesystem before S3 existed.
    probes.add("storage", store.check_ready, critical=True)

    if dispatcher is not None:

        async def check_outbox() -> None:
            backlog = await dispatcher.backlog()
            if backlog > 5_000:
                raise RuntimeError(f"outbox backlog is {backlog}")

        # Non-critical: a file already stored is stored, and its announcement drains when the
        # broker returns. Failing readiness would stop uploads over a broker hiccup.
        probes.add("outbox", check_outbox, critical=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if cfg.run_migrations:
            applied = await migrate.run(strip_asyncpg_dsn(cfg.database_url), MIGRATIONS)
            logger.info("migrations up to date", extra={"applied": applied})

        # Before anything can use it, and before readiness can be asked. On S3 this opens the
        # client and makes sure the bucket is there; on the filesystem it does nothing.
        await store.start()

        # A crash mid-upload leaves something nothing references — a `.part` file on the
        # filesystem, an unfinished multipart upload on S3. The second costs storage until it is
        # aborted and nothing lists it by accident, so this is not tidiness. Boot is the one
        # moment clearing it is certainly safe.
        swept = await store.sweep_temporary_files()
        if swept:
            logger.info("cleared abandoned uploads", extra={"count": swept})

        if producer is not None:
            await producer.start()
            if cfg.kafka_ensure_topics:
                await kafka.ensure_topics(
                    cfg.kafka_brokers,
                    cfg.owned_topics,
                    partitions=cfg.kafka_topic_partitions,
                    replication=cfg.kafka_topic_replication,
                )
            await dispatcher.start()  # type: ignore[union-attr]

        try:
            async with sessions() as session:
                from app.platform.db import session_var

                token = session_var.set(session)
                try:
                    stored_bytes.set(await repository.total_bytes())
                finally:
                    session_var.reset(token)
        except Exception:
            # A metric is not worth failing a boot over.
            logger.warning("could not read the initial stored-bytes total", exc_info=True)

        logger.info(
            "media-service started",
            extra={
                "environment": cfg.environment,
                # The backend, and only the location that backend actually uses. Logging
                # `storage_root` unconditionally said `/var/lib/arcadia/media` while every byte
                # was going to a bucket — a line that answers "where are my files" wrongly is
                # worse than one that does not answer it.
                "storage_backend": cfg.storage_backend,
                "storage_location": (
                    f"{cfg.s3_endpoint_url}/{cfg.s3_bucket}"
                    if cfg.storage_backend == "s3"
                    else cfg.storage_root
                ),
                "kafka": cfg.kafka_enabled,
                "port": cfg.http_port,
            },
        )
        try:
            yield
        finally:
            if dispatcher is not None:
                await dispatcher.stop()
            if producer is not None:
                await producer.stop()
            # After the dispatcher and producer, before the engine: an in-flight download is
            # reading through this client, and closing it first would abort the response.
            await store.aclose()
            await engine.dispose()
            logger.info("media-service stopped")

    app = FastAPI(
        title="Arcadia Media Service",
        version=cfg.service_version,
        description=(
            "The platform's only file store. Uploads are type-checked against their actual "
            "bytes; private files are served through short-lived signed URLs."
        ),
        lifespan=lifespan,
        docs_url="/docs" if not cfg.is_production else None,
        redoc_url=None,
    )

    app.state.config = cfg
    app.state.verifier = Verifier(
        secret=cfg.jwt_secret,
        public_key=cfg.jwt_public_key,
        algorithm=cfg.jwt_algorithm,
        issuer=cfg.jwt_issuer,
        audience=cfg.jwt_audience,
    )
    app.state.media_service = media_service
    app.state.object_store = store

    install_middleware(app, service=cfg.service_name)
    install_error_handlers(app)
    install_operational_routes(app, readiness=probes.report)

    app.include_router(media_routes.router)

    return app
