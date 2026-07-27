"""Wiring for the HTTP layer."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.application.media_service import MediaService
from app.platform.auth import Principal, Verifier, current_principal


def media(request: Request) -> MediaService:
    return request.app.state.media_service


_optional_bearer = HTTPBearer(auto_error=False)


async def optional_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_optional_bearer),
) -> Principal | None:
    """The caller, if there is one.

    Public assets are readable without a login — a storefront image behind a login is an image
    nobody sees — so these routes cannot require a token. But a token that *is* present must
    still be valid: silently ignoring a bad one would mean a caller with an expired session
    being quietly treated as anonymous, and getting a confusing 404 for their own private file.
    """
    if credentials is None or not credentials.credentials:
        return None
    verifier: Verifier = request.app.state.verifier
    return verifier.verify(credentials.credentials)


class Pagination:
    def __init__(
        self,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
    ) -> None:
        self.limit = limit
        self.offset = offset


MediaServiceDep = Annotated[MediaService, Depends(media)]
PageDep = Annotated[Pagination, Depends(Pagination)]
CallerDep = Annotated[Principal, Depends(current_principal)]
OptionalCallerDep = Annotated[Principal | None, Depends(optional_principal)]
