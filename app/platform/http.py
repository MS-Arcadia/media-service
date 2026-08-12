"""The HTTP edge: middleware, error translation, health and metrics.

Everything here is transport concern only. A use case raises ``AppError``; this module
is the single place that decides an ``AppError`` becomes a 409 with an RFC 7807 body.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import AppError, Code
from .events import new_id
from .logging import correlation_id_var

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"

requests_total = Counter(
    "arcadia_http_requests_total",
    "HTTP requests handled.",
    ["service", "method", "route", "status"],
)
request_duration = Histogram(
    "arcadia_http_request_duration_seconds",
    "How long HTTP requests took.",
    ["service", "method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


def problem(
    status: int,
    title: str,
    detail: str,
    *,
    reason: str = "",
    extra: dict[str, object] | None = None,
) -> JSONResponse:
    """An RFC 7807 problem document.

    A machine-readable ``reason`` sits alongside the human-readable ``detail`` so a
    client can branch on the specific rule that was broken without parsing prose.
    """
    body: dict[str, object] = {
        "type": "about:blank",
        "title": title,
        "status": status,
        "detail": detail,
    }
    if reason:
        body["reason"] = reason
    if extra:
        body.update(extra)
    return JSONResponse(
        status_code=status,
        content=body,
        media_type="application/problem+json",
        headers={CORRELATION_HEADER: correlation_id_var.get()},
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(request: Request, exc: AppError) -> Response:
        # Only a genuine server-side fault deserves a stack trace and an ERROR line.
        # A 404 or a rejected request is the API working as designed; logging those at
        # ERROR is how an error log becomes something nobody reads.
        if exc.code is Code.INTERNAL:
            logger.exception("unhandled internal error", extra={"path": request.url.path})
        else:
            logger.info(
                "request rejected",
                extra={
                    "path": request.url.path,
                    "code": str(exc.code),
                    "reason": exc.reason,
                    "detail": exc.message,
                },
            )
        return problem(
            exc.http_status,
            str(exc.code),
            exc.message,
            reason=exc.reason,
            extra=exc.details or None,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> Response:
        fields = [
            {
                "field": ".".join(str(p) for p in err.get("loc", ())[1:]),
                "message": err.get("msg", ""),
            }
            for err in exc.errors()
        ]
        return problem(
            400,
            str(Code.INVALID_ARGUMENT),
            "the request body or parameters are not valid",
            reason="VALIDATION_FAILED",
            extra={"errors": fields},
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> Response:
        return problem(exc.status_code, "HTTP_ERROR", str(exc.detail))

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> Response:
        logger.exception("unhandled exception", extra={"path": request.url.path})
        # The message is deliberately generic: an exception string can carry a DSN, a
        # SQL fragment or a file path, none of which belongs in a client response.
        return problem(500, str(Code.INTERNAL), "the request could not be completed")


def install_middleware(app: FastAPI, *, service: str) -> None:
    @app.middleware("http")
    async def _observe(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # An inbound correlation id is trusted and reused; that is what stitches this
        # request to the caller's. Absent, one is minted here.
        correlation = request.headers.get(CORRELATION_HEADER) or new_id()
        token = correlation_id_var.set(correlation)
        started = time.perf_counter()
        # Everything that logs stays inside this try, and the reset is the only
        # thing in `finally`. That ordering is load-bearing: the access log below
        # reads the correlation id off this ContextVar, so resetting before it ran
        # left every access line — the one line per request a trace actually starts
        # from — with no id. Handler logs emitted during call_next carried it
        # correctly, which is what made the gap easy to miss.
        try:
            response = await call_next(request)
            elapsed = time.perf_counter() - started

            # The route template, never the concrete path: labelling by path would
            # mint a new time series per order id and eventually take Prometheus down.
            route = request.scope.get("route")
            route_label = getattr(route, "path", request.url.path)

            requests_total.labels(
                service, request.method, route_label, str(response.status_code)
            ).inc()
            request_duration.labels(service, request.method, route_label).observe(elapsed)

            response.headers[CORRELATION_HEADER] = correlation
            if request.url.path not in ("/livez", "/readyz", "/metrics"):
                logger.info(
                    "request",
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "status": response.status_code,
                        "duration_ms": round(elapsed * 1000, 2),
                    },
                )
            return response
        finally:
            correlation_id_var.reset(token)


def install_operational_routes(app: FastAPI, *, readiness: Callable[[], Awaitable[dict]]) -> None:
    """Mount /livez, /readyz and /metrics.

    Liveness deliberately checks nothing. Readiness checks dependencies. Conflating
    them means a database blip restarts every replica, which turns a brief outage into
    a long one.
    """

    @app.get("/livez", include_in_schema=False)
    async def livez() -> dict[str, str]:
        return {"status": "UP"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> Response:
        report = await readiness()
        code = 200 if report.get("status") != "DOWN" else 503
        return JSONResponse(status_code=code, content=report)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
