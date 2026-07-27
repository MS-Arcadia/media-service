"""The error taxonomy.

The domain and application layers raise these; the edge translates them. A REST
handler turns one into an RFC 7807 problem document, a Kafka handler asks
``retryable`` whether to retry or dead-letter. Neither layer needs to know what the
other does with it, which is the point of having a taxonomy rather than HTTP status
codes scattered through use cases.
"""

from __future__ import annotations

from enum import StrEnum


class Code(StrEnum):
    """What kind of failure this is, independent of transport."""

    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    NOT_FOUND = "NOT_FOUND"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    CONFLICT = "CONFLICT"
    FAILED_PRECONDITION = "FAILED_PRECONDITION"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    UNAVAILABLE = "UNAVAILABLE"
    INTERNAL = "INTERNAL"


_HTTP_STATUS: dict[Code, int] = {
    Code.INVALID_ARGUMENT: 400,
    Code.UNAUTHENTICATED: 401,
    Code.PERMISSION_DENIED: 403,
    Code.NOT_FOUND: 404,
    Code.ALREADY_EXISTS: 409,
    Code.CONFLICT: 409,
    Code.FAILED_PRECONDITION: 422,
    Code.RESOURCE_EXHAUSTED: 429,
    Code.UNAVAILABLE: 503,
    Code.INTERNAL: 500,
}

# Only these are worth trying again. A business rejection will be rejected just as
# firmly on the second attempt, so retrying it wastes the retry budget and delays
# the dead-letter that tells an operator something is actually wrong.
_RETRYABLE: frozenset[Code] = frozenset({Code.UNAVAILABLE, Code.INTERNAL})


class AppError(Exception):
    """A failure with a classification.

    ``reason`` is a stable machine-readable code for the specific rule that was
    broken (``GAME_NOT_PUBLISHED``, ``REFUND_WINDOW_CLOSED``). Clients branch on it;
    ``message`` is for humans and may be reworded freely.
    """

    def __init__(
        self,
        code: Code,
        message: str,
        *,
        reason: str = "",
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.reason = reason
        self.details = details or {}

    @property
    def http_status(self) -> int:
        return _HTTP_STATUS.get(self.code, 500)

    @property
    def retryable(self) -> bool:
        return self.code in _RETRYABLE

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def _factory(code: Code):
    def make(message: str, *, reason: str = "", **details: object) -> AppError:
        return AppError(code, message, reason=reason, details=details or None)

    return make


invalid_argument = _factory(Code.INVALID_ARGUMENT)
unauthenticated = _factory(Code.UNAUTHENTICATED)
permission_denied = _factory(Code.PERMISSION_DENIED)
not_found = _factory(Code.NOT_FOUND)
already_exists = _factory(Code.ALREADY_EXISTS)
conflict = _factory(Code.CONFLICT)
failed_precondition = _factory(Code.FAILED_PRECONDITION)
resource_exhausted = _factory(Code.RESOURCE_EXHAUSTED)
unavailable = _factory(Code.UNAVAILABLE)
internal = _factory(Code.INTERNAL)


def is_retryable(exc: BaseException) -> bool:
    """Whether a Kafka handler should retry this failure rather than dead-letter it.

    An unclassified exception is treated as retryable: it is more likely a bug or a
    transient infrastructure fault than a considered business rejection, and a
    retry is cheaper to recover from than a message quietly parked in the DLQ.
    """
    if isinstance(exc, AppError):
        return exc.retryable
    return True
