"""The only place that chooses concrete infrastructure.

Notably: this is where ``ObjectStore`` becomes a directory on disk. Swapping in MinIO or S3
is a different class on one line here.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Gauge
from sqlalchemy import text

from app.adapters.inbound.rest import media as media_routes
from app.adapters.outbound.filesystem import FilesystemObjectStore
from app.adapters.outbound.publisher import OutboxPublisher
from app.adapters.outbound.repositories import PostgresMediaRepository
from app.application.download_token import TokenIssuer
from app.application.media_service import MediaService
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

    def _probe_storage() -> None:
        """Write a byte and remove it again.

        A read-only mount and a missing volume both look fine to a directory check and fail
        on the first upload, which is exactly the failure readiness exists to catch first.
        """
        root = Path(cfg.storage_root)
        if not root.is_dir():
            raise RuntimeError(f"{cfg.storage_root} is not a directory")
        probe = root / ".readyz"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()

    async def check_storage() -> None:
        # In a worker thread: these are blocking syscalls, and on a stalled network mount
        # they block for as long as the mount takes. Doing that on the event loop would
        # freeze every other request — turning a readiness check into an outage.
        await asyncio.to_thread(_probe_storage)

    probes.add("storage", check_storage, critical=True)

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

        # A crash between opening a temporary file and renaming it leaves a partial write that
        # nothing references. Cleared at boot, which is the one moment it is certainly safe.
        swept = await store.sweep_temporary_files()
        if swept:
            logger.info("removed leftover partial uploads", extra={"count": swept})

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
                "storage_root": cfg.storage_root,
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

    if cfg.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cfg.cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "DELETE"],
            allow_headers=["Authorization", "Content-Type", "X-Correlation-ID"],
            expose_headers=["ETag", "Content-Disposition"],
        )

    install_middleware(app, service=cfg.service_name)
    install_error_handlers(app)
    install_operational_routes(app, readiness=probes.report)

    app.include_router(media_routes.router)

    return app
