"""JWT verification and role checks.

Every service on the platform verifies with the same material, so a token minted by
Auth works here unchanged. Two details matter:

* The algorithm is **pinned**. Accepting whatever the token's own header claims is
  the classic JWT forgery: an attacker sends ``alg: none``, or signs an RS256-expecting
  verifier's token with its public key as an HMAC secret.
* A **refresh token is never a credential**. It is long-lived by design, so accepting
  one for an API call would hand out a long-lived API key.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

import jwt
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import errors


class Role(StrEnum):
    """The platform's four roles. Each user has exactly one."""

    BASIC_USER = "BASIC_USER"
    DEVELOPER = "DEVELOPER"
    SUPPORT = "SUPPORT"
    ADMIN = "ADMIN"


# Staff can do anything Support can do, so this is written once rather than repeated
# at every call site as {SUPPORT, ADMIN}.
STAFF = frozenset({Role.SUPPORT, Role.ADMIN})


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling."""

    user_id: str
    role: Role
    email: str = ""
    scopes: frozenset[str] = field(default_factory=frozenset)

    @property
    def is_staff(self) -> bool:
        return self.role in STAFF

    def has_role(self, *roles: Role) -> bool:
        return self.role in roles


class Verifier:
    """Verifies access tokens with a pinned algorithm."""

    def __init__(
        self,
        *,
        secret: str = "",
        public_key: str = "",
        algorithm: str = "HS256",
        issuer: str = "",
        audience: str = "",
        leeway: int = 30,
    ) -> None:
        if algorithm.startswith("HS"):
            if not secret:
                raise ValueError("a symmetric algorithm needs JWT_SECRET")
            self._key: str = secret
        elif algorithm.startswith("RS"):
            if not public_key:
                raise ValueError("an asymmetric algorithm needs JWT_PUBLIC_KEY")
            self._key = public_key
        else:
            raise ValueError(f"unsupported JWT algorithm {algorithm!r}")

        self._algorithm = algorithm
        self._issuer = issuer
        self._audience = audience
        self._leeway = leeway

    def verify(self, token: str) -> Principal:
        options = {
            "require": ["exp", "sub"],
            "verify_aud": bool(self._audience),
        }
        try:
            claims = jwt.decode(
                token,
                self._key,
                algorithms=[self._algorithm],
                issuer=self._issuer or None,
                audience=self._audience or None,
                leeway=self._leeway,
                options=options,
            )
        except jwt.ExpiredSignatureError as exc:
            raise errors.unauthenticated("the token has expired", reason="TOKEN_EXPIRED") from exc
        except jwt.InvalidTokenError as exc:
            raise errors.unauthenticated("the token is not valid", reason="TOKEN_INVALID") from exc

        # A whitelist, not a blacklist. This used to refuse only `typ == "refresh"`, so a token
        # that said nothing about itself was assumed to be an access token — and the auth service
        # spelled the claim `type`, which meant its seven-day refresh tokens, carrying a full
        # role, were accepted on every endpoint here. Requiring the claim closes that for any
        # future issuer too, not just the one that got it wrong.
        # S105 is a false positive here: bandit sees a name ending in `_type` beside a literal and
        # guesses "hardcoded password". This is a JWT claim value, and "access" is the contract.
        # The two literals below carry `noqa: S105`, which is bandit guessing: it sees a name
        # containing "token" compared against a string and reports a hardcoded password. These are
        # JWT claim values, and they are the contract with the auth service.
        token_type = str(claims.get("typ") or "")
        if token_type != "access":  # noqa: S105
            raise errors.unauthenticated(
                "only an access token may be used to call the API",
                reason=(
                    "REFRESH_TOKEN_USED" if token_type == "refresh" else "WRONG_TOKEN_TYPE"  # noqa: S105
                ),
            )

        subject = claims.get("sub") or ""
        if not subject:
            raise errors.unauthenticated("the token carries no subject", reason="TOKEN_INVALID")

        raw_role = str(claims.get("role") or "")
        try:
            role = Role(raw_role)
        except ValueError as exc:
            raise errors.unauthenticated(
                f"unknown role {raw_role!r}", reason="TOKEN_INVALID"
            ) from exc

        scopes = claims.get("scopes") or []
        return Principal(
            user_id=subject,
            role=role,
            email=str(claims.get("email") or ""),
            scopes=frozenset(str(s) for s in scopes),
        )


_bearer = HTTPBearer(auto_error=False)


async def current_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal:
    """FastAPI dependency: the verified caller, or 401."""
    if credentials is None or not credentials.credentials:
        raise errors.unauthenticated("an access token is required", reason="TOKEN_MISSING")
    verifier: Verifier | None = getattr(request.app.state, "verifier", None)
    if verifier is None:
        raise errors.internal("the token verifier is not configured")
    return verifier.verify(credentials.credentials)


def require(*roles: Role):
    """FastAPI dependency factory: the caller must hold one of these roles.

    Authorisation lives in a dependency rather than inside each handler so that a new
    endpoint cannot be added without stating who may call it.
    """

    async def dependency(principal: Principal = Depends(current_principal)) -> Principal:
        if roles and not principal.has_role(*roles):
            raise errors.permission_denied(
                f"this action requires one of: {', '.join(sorted(r.value for r in roles))}",
                reason="ROLE_REQUIRED",
            )
        return principal

    return dependency


require_staff = require(Role.SUPPORT, Role.ADMIN)
require_developer = require(Role.DEVELOPER)
require_admin = require(Role.ADMIN)
