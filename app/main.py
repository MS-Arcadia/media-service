"""ASGI entrypoint.

uvicorn app.main:app
"""

from app.bootstrap import build

app = build()
