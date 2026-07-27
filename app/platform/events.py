"""The Kafka wire envelope, and the transactional outbox that fills it.

The envelope is byte-compatible with the one the Go services publish: the wallet
service deduplicates on ``event_id``, decides on ``schema_version`` whether it can
decode the payload at all, and joins log lines across services on ``trace_id``. A
field renamed here silently breaks a consumer nobody in this repository owns, so the
shape is fixed by contract rather than by convenience.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1


def new_id() -> str:
    """A fresh identifier.

    UUIDv4 rather than v7: nothing here sorts by id, and v7 leaks a timestamp into
    every public identifier for no benefit we use.
    """
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class Envelope:
    """One message on the bus."""

    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]
    producer: str
    event_id: str = field(default_factory=new_id)
    schema_version: int = SCHEMA_VERSION
    occurred_at: datetime = field(default_factory=utcnow)
    correlation_id: str = ""
    causation_id: str = ""
    trace_id: str = ""

    @property
    def partition_key(self) -> str:
        """Keying by aggregate is what preserves per-order and per-game ordering.

        Two events about the same order must never be processed out of sequence by
        two consumers in the same group, and this is the only thing that guarantees
        it.
        """
        return self.aggregate_id

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "schema_version": self.schema_version,
            # RFC 3339 with a Z, which is what Go's time.Time unmarshals.
            "occurred_at": self.occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "producer": self.producer,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "payload": self.payload,
        }
        for key, value in (
            ("correlation_id", self.correlation_id),
            ("causation_id", self.causation_id),
            ("trace_id", self.trace_id),
        ):
            if value:
                out[key] = value
        return out

    def encode(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()

    @classmethod
    def decode(cls, raw: bytes | str) -> Envelope:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MalformedEnvelope(f"not JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise MalformedEnvelope("envelope must be a JSON object")

        for required in ("event_id", "event_type", "aggregate_id", "occurred_at"):
            if not data.get(required):
                raise MalformedEnvelope(f"{required} is required")

        version = data.get("schema_version", 0)
        if not isinstance(version, int) or version <= 0:
            raise MalformedEnvelope(f"unsupported schema version {version!r}")

        occurred = data["occurred_at"]
        try:
            parsed = datetime.fromisoformat(str(occurred).replace("Z", "+00:00"))
        except ValueError as exc:
            raise MalformedEnvelope(f"occurred_at {occurred!r} is not a timestamp") from exc

        payload = data.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise MalformedEnvelope("payload must be a JSON object")

        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            schema_version=version,
            occurred_at=parsed,
            producer=data.get("producer", ""),
            aggregate_type=data.get("aggregate_type", ""),
            aggregate_id=data["aggregate_id"],
            correlation_id=data.get("correlation_id", ""),
            causation_id=data.get("causation_id", ""),
            trace_id=data.get("trace_id", ""),
            payload=payload,
        )


class MalformedEnvelope(ValueError):
    """A message that is not a valid envelope.

    Permanent by nature: redelivering the same bytes produces the same failure, so
    these go straight to the dead-letter topic.
    """


class EnvelopeFactory:
    """Stamps the producer name and schema version so use cases supply only the rest."""

    def __init__(self, producer: str) -> None:
        self._producer = producer

    def build(
        self,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        *,
        correlation_id: str = "",
        causation_id: str = "",
        trace_id: str = "",
    ) -> Envelope:
        return Envelope(
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
            producer=self._producer,
            correlation_id=correlation_id,
            causation_id=causation_id,
            trace_id=trace_id,
        )
