"""Structured logging with a correlation id.

There is no tracing backend. Instead one identifier is propagated through an HTTP
header and the Kafka envelope, and stamped on every line. That answers the question
this platform actually needs answered — "show me every log line for this purchase,
across all five services" — for the cost of one string and no collector to run.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")

# Anything whose key looks like one of these is replaced rather than logged. A key
# name is a blunt instrument, but it catches the realistic accident: a request body
# logged wholesale during debugging.
_REDACT = ("password", "secret", "token", "authorization", "api_key", "pepper")

_RESERVED = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
    }
)


def _redact(key: str, value: object) -> object:
    lowered = key.lower()
    if any(marker in lowered for marker in _REDACT):
        return "[redacted]"
    return value


class JSONFormatter(logging.Formatter):
    """One JSON object per line, which is what makes the logs greppable by field."""

    def __init__(self, service: str, version: str) -> None:
        super().__init__()
        self._service = service
        self._version = version

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
            "service": self._service,
            "version": self._version,
        }
        correlation = correlation_id_var.get()
        if correlation:
            entry["correlation_id"] = correlation

        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                entry[key] = _redact(key, value)

        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str, separators=(",", ":"))


def configure(*, service: str, version: str, level: str = "INFO", json_format: bool = True) -> None:
    """Install the root handler. Called once, at boot."""
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    if json_format:
        handler.setFormatter(JSONFormatter(service, version))
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(name)s: %(message)s"))
    root.addHandler(handler)

    # uvicorn's access log duplicates what the request middleware already records,
    # with none of the correlation id, so it is silenced rather than reformatted.
    logging.getLogger("uvicorn.access").handlers.clear()
    logging.getLogger("uvicorn.access").propagate = False
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
