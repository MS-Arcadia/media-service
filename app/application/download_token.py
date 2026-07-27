"""Short-lived signed download URLs — the local equivalent of an S3 presigned URL.

The architecture document specifies MinIO, whose presigned URLs let a client fetch an object
directly with no further authorisation. This service stores files itself, so the same
property is provided the same way: a URL that carries its own proof, expires, and is bound
to one object.

Why not just require the Authorization header on the download route? Because a download is
often not made by an API client. It goes into an `<img src>`, a `<video>`, or a download
manager, none of which will attach a bearer token. Without this, a private file could only be
fetched by proxying it through something that can — or by making it public.

Three properties:

* **Signed**, with HMAC-SHA256 over the fields, so nothing in the URL can be edited.
* **Expiring**, so a leaked URL in a browser history or a referrer header stops working.
* **Bound** to one media id and one subject, so a token for a screenshot cannot fetch a
  game build.
"""

from __future__ import annotations

import base64
import hmac
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from app.platform import errors

DEFAULT_TTL = timedelta(minutes=15)

# Reason codes, not credentials. The linter flags any constant with "TOKEN" in its name as
# a possible hardcoded secret; these are the strings a client branches on.
REASON_TOKEN_INVALID = "DOWNLOAD_TOKEN_INVALID"  # noqa: S105
REASON_TOKEN_EXPIRED = "DOWNLOAD_TOKEN_EXPIRED"  # noqa: S105


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _unb64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


class TokenIssuer:
    """Mints and verifies download tokens."""

    def __init__(self, secret: str, *, ttl: timedelta = DEFAULT_TTL) -> None:
        if len(secret) < 32:
            raise ValueError("the media download secret must be at least 32 characters")
        self._secret = secret.encode()
        self._ttl = ttl

    @property
    def ttl_seconds(self) -> int:
        return int(self._ttl.total_seconds())

    def issue(self, *, media_id: str, subject: str, now: datetime) -> str:
        claims = {
            "m": media_id,
            # Who it was issued to. Recorded so an abused token can be traced back, and so a
            # token cannot be handed to somebody else and reused indefinitely.
            "s": subject,
            "e": int((now + self._ttl).timestamp()),
        }
        body = _b64(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
        return f"{body}.{self._sign(body)}"

    def verify(self, token: str, *, media_id: str, now: datetime) -> str:
        """Check a token and return the subject it was issued to."""
        parts = token.split(".")
        if len(parts) != 2:
            raise errors.permission_denied(
                "the download token is malformed", reason=REASON_TOKEN_INVALID
            )
        body, signature = parts

        # Constant time. A byte-by-byte `==` leaks how much of a forged signature was
        # correct, which is enough to reconstruct it one byte at a time.
        if not hmac.compare_digest(signature, self._sign(body)):
            raise errors.permission_denied(
                "the download token is not valid", reason=REASON_TOKEN_INVALID
            )

        try:
            claims = json.loads(_unb64(body))
        except (ValueError, TypeError) as exc:
            raise errors.permission_denied(
                "the download token is malformed", reason=REASON_TOKEN_INVALID
            ) from exc

        # Checked after the signature, so an attacker cannot use the error messages to
        # explore what the payload should look like.
        if claims.get("m") != media_id:
            raise errors.permission_denied(
                "this download token is for a different file", reason=REASON_TOKEN_INVALID
            )

        expires = claims.get("e")
        if not isinstance(expires, int):
            raise errors.permission_denied(
                "the download token has no expiry", reason=REASON_TOKEN_INVALID
            )
        if datetime.fromtimestamp(expires, UTC) <= now:
            raise errors.permission_denied(
                "the download token has expired", reason=REASON_TOKEN_EXPIRED
            )

        return str(claims.get("s") or "")

    def _sign(self, body: str) -> str:
        return _b64(hmac.new(self._secret, body.encode(), sha256).digest())
