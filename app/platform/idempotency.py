"""Idempotency for money-moving requests.

A purchase must survive a double-clicked button and a client that retries on timeout.
The mechanism is claim-then-save: the key is inserted first, so a concurrent duplicate
collides on the primary key instead of starting a second purchase, and the response is
stored once the work is done so a later retry can replay it.

A retry carrying a **different** body under the same key is rejected rather than
answered from the store. That is a client bug, and quietly returning the old answer
would hide it.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import DateTime, String, Text, func, select
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from . import errors
from .db import Base


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    resource_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


def hash_request(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


class Store:
    """Claims keys and replays stored responses."""

    def __init__(self, *, in_flight_grace: timedelta = timedelta(seconds=30)) -> None:
        self._grace = in_flight_grace

    async def lookup(self, session: AsyncSession, *, key: str, request: Any) -> dict | None:
        """Return the stored response for ``key``, without claiming it.

        A read-only pre-check, for use *before* a use case does expensive work that a replay
        would make pointless — a call to another service, say.

        It is not a substitute for ``claim``: two concurrent first attempts would both find
        nothing here and both proceed. ``claim`` is still what makes the work happen once. This
        only short-circuits the common case, where the client is retrying something that has
        already finished.

        A body that disagrees with the stored one is rejected here too, so a client bug is
        reported before anything else is attempted rather than after.
        """
        if not key:
            raise errors.invalid_argument(
                "an Idempotency-Key header is required for this request",
                reason="IDEMPOTENCY_KEY_REQUIRED",
            )
        existing = (
            await session.execute(select(IdempotencyKey).where(IdempotencyKey.key == key))
        ).scalar_one_or_none()
        if existing is None or existing.completed_at is None:
            return None
        if existing.request_hash != hash_request(request):
            raise errors.conflict(
                "this Idempotency-Key was already used with a different request body",
                reason="IDEMPOTENCY_KEY_REUSED",
            )
        stored = dict(existing.response or {})
        stored["idempotent_replay"] = True
        return stored

    async def claim(
        self, session: AsyncSession, *, key: str, scope: str, request: Any
    ) -> dict | None:
        """Claim ``key``, or return the stored response if it was already used.

        Returns ``None`` when the caller owns the key and should do the work.
        """
        if not key:
            raise errors.invalid_argument(
                "an Idempotency-Key header is required for this request",
                reason="IDEMPOTENCY_KEY_REQUIRED",
            )

        digest = hash_request(request)

        # ON CONFLICT DO NOTHING makes the claim a single statement, so two concurrent
        # requests cannot both believe they won.
        result = await session.execute(
            insert(IdempotencyKey)
            .values(key=key, scope=scope, request_hash=digest)
            .on_conflict_do_nothing(index_elements=[IdempotencyKey.key])
            .returning(IdempotencyKey.key)
        )
        if result.scalar_one_or_none() is not None:
            return None

        existing = (
            await session.execute(select(IdempotencyKey).where(IdempotencyKey.key == key))
        ).scalar_one()

        if existing.request_hash != digest:
            raise errors.conflict(
                "this Idempotency-Key was already used with a different request body",
                reason="IDEMPOTENCY_KEY_REUSED",
            )

        if existing.completed_at is None:
            # The first attempt is still running. Telling the client to retry is
            # honest: answering now would mean either duplicating the work or
            # inventing a response for something still in flight.
            if datetime.now(UTC) - existing.created_at < self._grace:
                raise errors.conflict(
                    "a request with this Idempotency-Key is still in progress",
                    reason="IDEMPOTENCY_KEY_IN_FLIGHT",
                )
            # Past the grace period the first attempt died before finishing. The key
            # is released so the retry can genuinely do the work.
            await session.delete(existing)
            await session.flush()
            await session.execute(
                insert(IdempotencyKey).values(key=key, scope=scope, request_hash=digest)
            )
            return None

        stored = dict(existing.response or {})
        stored["idempotent_replay"] = True
        return stored

    async def save(
        self,
        session: AsyncSession,
        *,
        key: str,
        response: dict,
        resource_id: str = "",
    ) -> None:
        record = (
            await session.execute(select(IdempotencyKey).where(IdempotencyKey.key == key))
        ).scalar_one()
        record.response = response
        record.resource_id = resource_id or None
        record.completed_at = datetime.now(UTC)
