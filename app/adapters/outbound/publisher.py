"""The outbox publisher."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.platform import outbox
from app.platform.events import EnvelopeFactory
from app.platform.logging import correlation_id_var


class OutboxPublisher:
    def __init__(self, *, producer_name: str, media_events_topic: str) -> None:
        self._factory = EnvelopeFactory(producer_name)
        self._topic = media_events_topic

    async def enqueue(
        self,
        session: AsyncSession,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        topic: str = "",
        causation_id: str = "",
    ) -> None:
        correlation = correlation_id_var.get()
        envelope = self._factory.build(
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
            correlation_id=correlation,
            trace_id=correlation,
            causation_id=causation_id,
        )
        await outbox.enqueue(session, topic or self._topic, envelope)
