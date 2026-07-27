"""Signed download URLs.

These are what stands between a private game build and the public internet, so the tests are
adversarial: each one is an attempt to get bytes without permission.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.download_token import TokenIssuer
from app.platform import errors

SECRET = "a-test-only-media-download-secret-32+"
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)


@pytest.fixture
def issuer() -> TokenIssuer:
    return TokenIssuer(SECRET, ttl=timedelta(minutes=15))


def test_a_freshly_issued_token_verifies(issuer: TokenIssuer):
    token = issuer.issue(media_id="media-1", subject="user-1", now=NOW)
    assert issuer.verify(token, media_id="media-1", now=NOW) == "user-1"


def test_a_token_for_one_file_cannot_fetch_another(issuer: TokenIssuer):
    """The attack that matters: a token for a screenshot must not fetch a game build."""
    token = issuer.issue(media_id="screenshot-1", subject="user-1", now=NOW)
    with pytest.raises(errors.AppError) as caught:
        issuer.verify(token, media_id="game-binary-1", now=NOW)
    assert caught.value.reason == "DOWNLOAD_TOKEN_INVALID"


def test_an_expired_token_is_refused(issuer: TokenIssuer):
    """A URL in a browser history or a referrer header stops working."""
    token = issuer.issue(media_id="media-1", subject="user-1", now=NOW)
    with pytest.raises(errors.AppError) as caught:
        issuer.verify(token, media_id="media-1", now=NOW + timedelta(minutes=16))
    assert caught.value.reason == "DOWNLOAD_TOKEN_EXPIRED"


def test_a_token_valid_just_before_expiry_still_works(issuer: TokenIssuer):
    token = issuer.issue(media_id="media-1", subject="user-1", now=NOW)
    assert issuer.verify(token, media_id="media-1", now=NOW + timedelta(minutes=14))


def test_a_tampered_payload_is_refused(issuer: TokenIssuer):
    """Editing the claims invalidates the signature, which is the whole point of signing."""
    import base64
    import json

    token = issuer.issue(media_id="media-1", subject="user-1", now=NOW)
    body, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    claims["m"] = "game-binary-1"
    forged_body = (
        base64.urlsafe_b64encode(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
        .decode()
        .rstrip("=")
    )

    with pytest.raises(errors.AppError) as caught:
        issuer.verify(f"{forged_body}.{signature}", media_id="game-binary-1", now=NOW)
    assert caught.value.reason == "DOWNLOAD_TOKEN_INVALID"


def test_an_extended_expiry_is_refused(issuer: TokenIssuer):
    """Pushing the expiry out is tampering like any other, and the signature catches it."""
    import base64
    import json

    token = issuer.issue(media_id="media-1", subject="user-1", now=NOW)
    body, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    claims["e"] = int((NOW + timedelta(days=365)).timestamp())
    forged_body = (
        base64.urlsafe_b64encode(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
        .decode()
        .rstrip("=")
    )

    with pytest.raises(errors.AppError):
        issuer.verify(f"{forged_body}.{signature}", media_id="media-1", now=NOW)


def test_a_token_signed_with_a_different_secret_is_refused(issuer: TokenIssuer):
    other = TokenIssuer("a-completely-different-secret-32-chars")
    token = other.issue(media_id="media-1", subject="user-1", now=NOW)
    with pytest.raises(errors.AppError) as caught:
        issuer.verify(token, media_id="media-1", now=NOW)
    assert caught.value.reason == "DOWNLOAD_TOKEN_INVALID"


@pytest.mark.parametrize(
    "token",
    ["", "garbage", "no-dot-here", "a.b.c", "....", "eyJ9.", ".signature"],
)
def test_a_malformed_token_is_refused_without_crashing(issuer: TokenIssuer, token: str):
    """A parser that raises something other than a clean 403 leaks a stack trace."""
    with pytest.raises(errors.AppError) as caught:
        issuer.verify(token, media_id="media-1", now=NOW)
    assert caught.value.http_status == 403


def test_a_token_with_no_expiry_is_refused():
    """Constructed by hand with a valid signature but no expiry claim — the one forgery a
    naive verifier would accept, because the signature really does check out."""
    import base64
    import hmac
    import json
    from hashlib import sha256

    issuer = TokenIssuer(SECRET)
    claims = {"m": "media-1", "s": "user-1"}
    body = (
        base64.urlsafe_b64encode(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode())
        .decode()
        .rstrip("=")
    )
    signature = (
        base64.urlsafe_b64encode(hmac.new(SECRET.encode(), body.encode(), sha256).digest())
        .decode()
        .rstrip("=")
    )

    with pytest.raises(errors.AppError) as caught:
        issuer.verify(f"{body}.{signature}", media_id="media-1", now=NOW)
    assert caught.value.reason == "DOWNLOAD_TOKEN_INVALID"


def test_a_short_secret_is_refused_at_construction():
    """Refused at boot rather than producing forgeable tokens at runtime."""
    with pytest.raises(ValueError, match="at least 32"):
        TokenIssuer("too-short")


def test_the_signature_is_compared_in_constant_time():
    """A byte-by-byte `==` leaks how much of a forged signature was correct, which is enough
    to reconstruct it one byte at a time. This asserts the implementation uses hmac.compare_digest.
    """
    import inspect

    source = inspect.getsource(TokenIssuer.verify)
    assert "compare_digest" in source
    assert "signature ==" not in source
