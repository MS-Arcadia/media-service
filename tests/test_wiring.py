"""Structural checks on the wiring.

These exist because of a specific failure that reached a running container twice: a method
appended to a service class after a module-level function landed *outside* the class, so the
route calling it raised `AttributeError` and returned 500. Every unit test passed — none of
them went through the router — and the end-to-end suite is what found it.

A route whose handler calls a method that does not exist is not a subtle bug. It should be
caught in a tenth of a second, not by starting eight containers.
"""

from __future__ import annotations

import inspect

import pytest
from fastapi.routing import APIRoute

from app.application.media_service import MediaService

# The one service the router dispatches to.
SERVICES = {"media": MediaService}


def routes() -> list[APIRoute]:
    """Every API route on the built app, flattened.

    FastAPI nests included routers, so `app.routes` alone does not reach them.
    """
    import os
    import tempfile

    os.environ.setdefault("DATABASE_URL", "postgresql://unused:unused@localhost:5432/unused")
    os.environ.setdefault("JWT_SECRET", "a-test-only-jwt-secret-at-least-32-chars")
    os.environ.setdefault("KAFKA_ENABLED", "false")
    os.environ.setdefault("RUN_MIGRATIONS", "false")
    os.environ.setdefault("DOWNLOAD_SECRET", "a-test-only-download-secret-32-chars-x")
    os.environ.setdefault("STORAGE_ROOT", tempfile.mkdtemp(prefix="arcadia-wiring-"))

    from app.bootstrap import build
    from app.config import get_config

    get_config.cache_clear()
    app = build()

    # FastAPI 0.14x wraps an included router in a `_IncludedRouter` whose routes hang off
    # `original_router`, so `app.routes` alone reaches only the operational endpoints. Walking
    # both shapes keeps this working whichever version is installed.
    found: list[APIRoute] = []
    pending: list[object] = list(app.routes)
    while pending:
        route = pending.pop()
        if isinstance(route, APIRoute):
            found.append(route)
            continue
        for attribute in ("original_router", "router"):
            nested = getattr(route, attribute, None)
            if nested is not None and hasattr(nested, "routes"):
                pending.extend(nested.routes)
        nested_list = getattr(route, "routes", None)
        if isinstance(nested_list, list):
            pending.extend(nested_list)
    return found


ALL_ROUTES = routes()


def test_the_app_exposes_routes():
    """A guard on the guard: if the flattening above breaks, every other test here would pass
    vacuously."""
    assert len(ALL_ROUTES) > 5, len(ALL_ROUTES)


@pytest.mark.parametrize("route", ALL_ROUTES, ids=lambda r: f"{sorted(r.methods)[0]} {r.path}")
def test_every_route_calls_a_method_that_exists(route: APIRoute):
    """The check that would have caught the 500.

    Reads each handler's source for `service.<name>(` and asserts the service class actually
    has that attribute. Crude, and it only sees direct calls — which is exactly the shape the
    bug took.
    """
    import re

    source = inspect.getsource(route.endpoint)
    for called in set(re.findall(r"\b(?:service|queries)\.(\w+)\(", source)):
        matches = [cls for cls in SERVICES.values() if hasattr(cls, called)]
        assert matches, (
            f"{route.path} calls .{called}() but no service class has it.\n"
            f"If it was just added: check it is indented inside the class rather than after a "
            f"module-level function."
        )


@pytest.mark.parametrize("name,cls", SERVICES.items(), ids=list(SERVICES))
def test_no_service_method_leaked_to_module_level(name: str, cls: type):
    """A method defined after a module-level function silently becomes one itself.

    Asserts the module has no public coroutine at module level, which is what that mistake
    looks like.
    """
    import ast

    module = inspect.getmodule(cls)
    tree = ast.parse(inspect.getsource(module))
    leaked = [
        node.name
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and not node.name.startswith("_")
    ]
    assert not leaked, (
        f"{module.__name__} has public coroutines at module level: {leaked}. "
        f"These were probably meant to be methods of {cls.__name__}."
    )
