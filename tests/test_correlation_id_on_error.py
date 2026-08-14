"""The correlation id survives an unhandled exception.

`install_middleware`'s `_observe` used to set `correlation_id_var` and reset it in a bare
`finally` — which runs as the exception unwinds through this frame, *before* it reaches
`install_error_handlers`'s catch-all `Exception` handler. Starlette pulls a handler
registered for `Exception` out into `ServerErrorMiddleware`, which sits *outside* every
user middleware — so by the time that handler ran and logged "unhandled exception", the id
was already gone. An INFO line written from inside a successful request carried the real
id; the one ERROR line written for a genuine failure did not, and neither did the
`X-Correlation-ID` header on the 500 response the caller actually receives.

`JSONFormatter` reads `correlation_id_var.get()` at *format* time inside `emit()`, not as a
value stashed on the record — so proving this needs a handler that formats eagerly, the way
the real one does, rather than `caplog`'s raw records (which were never touched). TestClient
also runs the ASGI app through a separate portal thread with its own task, so evaluating the
ContextVar from the test's own context afterwards would read nothing that request ever set;
the assertion has to happen inside the handler, while the request's context is still live.

Reproduced directly against `install_middleware` + `install_error_handlers`, without the
real app's database or Kafka wiring — those two functions are the whole surface responsible
for the behaviour under test.
"""

from __future__ import annotations

import json
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.platform.http import CORRELATION_HEADER, install_error_handlers, install_middleware
from app.platform.logging import JSONFormatter


class _CapturingHandler(logging.Handler):
    """Formats every record through the real `JSONFormatter`, immediately, in whatever
    context is current when the log call happens — exactly what production does."""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JSONFormatter(service="test-service", version="test"))
        self.lines: list[dict[str, object]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(json.loads(self.format(record)))


@pytest.fixture
def captured() -> _CapturingHandler:
    handler = _CapturingHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        root.removeHandler(handler)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    install_middleware(app, service="test-service")
    install_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("deliberate failure for the test")

    @app.get("/ok")
    async def ok() -> dict[str, str]:
        return {"status": "fine"}

    # Exceptions must reach the app's own ServerErrorMiddleware, exactly as they do in
    # production — the default would have TestClient re-raise them straight to the test,
    # which never exercises the code path this bug lives in.
    return TestClient(app, raise_server_exceptions=False)


def test_the_error_response_echoes_the_caller_s_correlation_id(client: TestClient):
    sent = "test-correlation-abc123"
    response = client.get("/boom", headers={CORRELATION_HEADER: sent})

    assert response.status_code == 500
    assert response.headers[CORRELATION_HEADER] == sent


def test_the_error_log_line_carries_the_same_id(client: TestClient, captured: _CapturingHandler):
    sent = "test-correlation-xyz789"
    client.get("/boom", headers={CORRELATION_HEADER: sent})

    errors = [line for line in captured.lines if line["level"] in ("ERROR", "CRITICAL")]
    assert errors, "the unhandled-exception handler must log something"
    logged_ids = {line.get("correlation_id") for line in errors}
    assert sent in logged_ids, (
        f"none of the error log lines carried the request's correlation id "
        f"(saw {logged_ids!r}) — it was reset before the handler that logs it ran"
    )


def test_a_successful_request_still_gets_its_id_back(client: TestClient):
    sent = "test-correlation-ok"
    response = client.get("/ok", headers={CORRELATION_HEADER: sent})

    assert response.status_code == 200
    assert response.headers[CORRELATION_HEADER] == sent
