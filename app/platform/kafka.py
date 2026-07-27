"""Kafka: a producer, and a consumer that knows the difference between a bug and a no.

The consumer's retry policy is the part worth reading. A business rejection is
permanent — the same message will be rejected just as firmly in a second — so it goes
straight to the dead-letter topic where an operator can see it. Only infrastructure
failures are retried. Getting this backwards produces a service that retries an
invalid message forever and never reports it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from .errors import is_retryable
from .events import Envelope, MalformedEnvelope
from .logging import correlation_id_var

logger = logging.getLogger(__name__)

Handler = Callable[[Envelope], Awaitable[None]]


class Producer:
    """Publishes envelopes.

    ``acks="all"`` and ``enable_idempotence`` are not tuning knobs here. This carries
    saga commands that move money; a message acknowledged by a leader that then dies
    before replicating is a debit nobody will ever perform.
    """

    def __init__(self, brokers: list[str], client_id: str) -> None:
        self._brokers = brokers
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            client_id=self._client_id,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            request_timeout_ms=15000,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, *, key: str, value: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("producer is not started")
        await self._producer.send_and_wait(
            topic,
            key=key.encode() if key else None,
            value=json.dumps(value, separators=(",", ":")).encode(),
        )


class Router:
    """Dispatches an envelope to the handler registered for its type."""

    def __init__(self, *, dead_letter_unknown: bool = False) -> None:
        self._handlers: dict[str, Handler] = {}
        self._dead_letter_unknown = dead_letter_unknown

    def on(self, event_type: str, handler: Handler) -> Router:
        self._handlers[event_type] = handler
        return self

    @property
    def dead_letter_unknown(self) -> bool:
        return self._dead_letter_unknown

    def handler_for(self, event_type: str) -> Handler | None:
        return self._handlers.get(event_type)


class UnknownMessage(Exception):
    """No handler is registered for this event type."""


class Consumer:
    """One consumer group over one topic.

    Offsets are committed manually, after the handler has succeeded or the message has
    been dead-lettered. Auto-commit would acknowledge a message the moment it was
    fetched, so a crash mid-handler would lose it — and this consumes the replies that
    drive a purchase saga forward.
    """

    def __init__(
        self,
        brokers: list[str],
        *,
        topic: str,
        group_id: str,
        router: Router,
        producer: Producer,
        dlq_suffix: str = ".dlq",
        max_retries: int = 3,
        retry_backoff: float = 1.0,
    ) -> None:
        self._brokers = brokers
        self._topic = topic
        # The group is scoped to the topic, and that is not cosmetic.
        #
        # A Kafka consumer group has ONE subscription shared by all its members. Two consumers
        # in the same group subscribing to different topics is a misconfiguration: the
        # coordinator assigns partitions from the *union* of the subscriptions, so a partition
        # of topic A can be handed to the member that only subscribed to topic B — which
        # ignores it. Nothing errors. The messages are simply never processed.
        #
        # This service hit exactly that: one group with a consumer on wallet-events and another
        # on game-events. game-events committed normally, wallet-events never committed a
        # single offset, and every purchase sat in PENDING with no error in any log.
        #
        # Deriving the group from the topic makes the two cases come out right on their own:
        # different topics get different groups, and two consumers on the *same* topic still
        # share one group and split its partitions, which is what scaling out should do.
        self._group_id = f"{group_id}.{topic}"
        self._router = router
        self._producer = producer
        self._dlq_topic = f"{topic}{dlq_suffix}"
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._brokers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_interval_ms=300000,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._loop(), name=f"consumer-{self._topic}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def _loop(self) -> None:
        assert self._consumer is not None
        try:
            async for message in self._consumer:
                try:
                    await self._handle_with_retries(message.value)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("giving up on a message from %s", self._topic)
                await self._consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("consumer loop for %s stopped", self._topic)

    async def _handle_with_retries(self, raw: bytes) -> None:
        try:
            envelope = Envelope.decode(raw)
        except MalformedEnvelope as exc:
            # Nothing about redelivering the same bytes will make them parse.
            await self._dead_letter(raw, f"malformed envelope: {exc}")
            return

        token = correlation_id_var.set(
            envelope.trace_id or envelope.correlation_id or envelope.event_id
        )
        try:
            handler = self._router.handler_for(envelope.event_type)
            if handler is None:
                if self._router.dead_letter_unknown:
                    await self._dead_letter(raw, f"no handler for {envelope.event_type}")
                else:
                    logger.debug("ignoring %s on %s", envelope.event_type, self._topic)
                return

            attempt = 0
            while True:
                try:
                    await handler(envelope)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if not is_retryable(exc):
                        await self._dead_letter(raw, f"{type(exc).__name__}: {exc}")
                        return
                    attempt += 1
                    if attempt > self._max_retries:
                        await self._dead_letter(
                            raw, f"still failing after {self._max_retries} retries: {exc}"
                        )
                        return
                    logger.warning(
                        "retrying %s (attempt %d/%d): %s",
                        envelope.event_type,
                        attempt,
                        self._max_retries,
                        exc,
                    )
                    await asyncio.sleep(self._retry_backoff * attempt)
        finally:
            correlation_id_var.reset(token)

    async def _dead_letter(self, raw: bytes, reason: str) -> None:
        logger.error("dead-lettering a message from %s: %s", self._topic, reason)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", errors="replace")}
        await self._producer.send(
            self._dlq_topic,
            key=str(payload.get("event_id", "unknown")) if isinstance(payload, dict) else "unknown",
            value={
                "original": payload,
                "reason": reason,
                "source_topic": self._topic,
                "consumer_group": self._group_id,
            },
        )


async def ensure_topics(
    brokers: list[str], topics: list[str], *, partitions: int, replication: int
) -> None:
    """Create the topics this service owns.

    Broker-side auto-creation is off in the compose file on purpose: with it on, a
    typo in a topic name silently creates a topic nobody produces to, and the bug
    surfaces later as a consumer that simply never receives anything.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        wanted = [
            NewTopic(name=t, num_partitions=partitions, replication_factor=replication)
            for t in topics
        ]
        with contextlib.suppress(TopicAlreadyExistsError):
            await admin.create_topics(wanted)
    finally:
        await admin.close()
