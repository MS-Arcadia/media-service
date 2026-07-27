"""The transactional outbox.

The problem it solves: a state change belongs in Postgres and its announcement
belongs in Kafka, and there is no transaction spanning both. Writing to Kafka first
risks announcing something that then fails to persist; writing to Postgres first
risks a state change nobody hears about.

So nothing is published directly. A use case appends the event to a table in its own
transaction — one commit, both facts — and a dispatcher drains that table afterwards.
Delivery becomes at-least-once, which is why every consumer in the platform
deduplicates on ``event_id``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Integer,
    String,
    Text,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base
from .events import Envelope

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 10


class OutboxMessage(Base):
    """One event waiting to be published."""

    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(128), nullable=False)
    envelope: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


async def enqueue(session: AsyncSession, topic: str, envelope: Envelope) -> None:
    """Append an event to the outbox using the caller's transaction.

    Taking the session as an argument rather than opening one is the whole design:
    it makes it impossible to write the event outside the transaction that produced
    it without noticing.
    """
    session.add(
        OutboxMessage(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            topic=topic,
            partition_key=envelope.partition_key,
            envelope=envelope.to_dict(),
        )
    )


class Dispatcher:
    """Drains the outbox onto Kafka.

    ``FOR UPDATE SKIP LOCKED`` is what lets several replicas run this at once: each
    claims a disjoint batch instead of contending for the same rows, so scaling the
    service out does not mean electing a leader.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        producer,
        *,
        interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._sessions = sessions
        self._producer = producer
        self._interval = interval
        self._batch_size = batch_size
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="outbox-dispatcher")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                published = await self.drain_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A failing dispatcher must not take the process down: the events are
                # still in the table and the next tick tries again.
                logger.exception("outbox dispatch failed")
                published = 0
            # Only pause when there was nothing to do, so a backlog drains at full
            # speed instead of one batch per interval.
            if published < self._batch_size:
                await asyncio.sleep(self._interval)

    async def drain_once(self) -> int:
        """Publish one batch. Returns how many were published."""
        async with self._sessions() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(OutboxMessage)
                        .where(
                            OutboxMessage.published_at.is_(None),
                            OutboxMessage.attempts < MAX_ATTEMPTS,
                        )
                        .order_by(OutboxMessage.id)
                        .limit(self._batch_size)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )

            published = 0
            for row in rows:
                try:
                    await self._producer.send(
                        row.topic,
                        key=row.partition_key,
                        value=row.envelope,
                    )
                except Exception as exc:
                    # Left unpublished with the attempt counted. After
                    # MAX_ATTEMPTS it stops being retried and stays in the table
                    # for an operator, rather than being dropped or retried
                    # forever.
                    row.attempts += 1
                    row.last_error = str(exc)[:1000]
                    logger.warning(
                        "could not publish %s (attempt %d): %s",
                        row.event_type,
                        row.attempts,
                        exc,
                    )
                    continue
                row.published_at = datetime.now(UTC)
                published += 1
            return published

    async def backlog(self) -> int:
        async with self._sessions() as session:
            result = await session.execute(
                text(
                    "SELECT count(*) FROM outbox_messages "
                    "WHERE published_at IS NULL AND attempts < :max"
                ),
                {"max": MAX_ATTEMPTS},
            )
            return int(result.scalar() or 0)
