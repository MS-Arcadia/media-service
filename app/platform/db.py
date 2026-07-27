"""PostgreSQL access, and the unit of work the outbox depends on.

There is exactly one rule in here worth remembering: an event is written inside the
same transaction as the state change that caused it. ``unit_of_work`` is what makes
that the easy thing to do — the session it yields is the one the repositories and
the outbox both use, so either both land or neither does.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from . import errors


class Base(DeclarativeBase):
    """Declarative base for every table in the service."""


# The session for the transaction currently in progress.
#
# A context variable rather than an argument threaded through every layer: a use case's
# signature should describe its domain, not carry a database handle down to a
# repository three calls away. Set only by UnitOfWork.begin(), so there is exactly one
# place it can come from.
session_var: ContextVar[AsyncSession | None] = ContextVar("db_session", default=None)


def current_session() -> AsyncSession:
    """The active session, or a clear failure.

    A repository called outside a transaction is a bug in wiring, not a runtime
    condition to handle, so this says so rather than quietly opening a session and
    committing on its own.
    """
    session = session_var.get()
    if session is None:
        raise errors.internal(
            "no database session is active; repository calls must happen inside uow.begin()"
        )
    return session


def create_engine(
    dsn: str, *, pool_size: int, max_overflow: int, echo: bool = False
) -> AsyncEngine:
    """Build the connection pool.

    ``pool_pre_ping`` costs a round trip on checkout and buys immunity to the
    connection Postgres closed while the service was idle — the classic first
    request after a quiet night failing for no reason the logs explain.
    """
    return create_async_engine(
        _normalise_dsn(dsn),
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=echo,
    )


def _normalise_dsn(dsn: str) -> str:
    """Accept the same DSN the Go services take.

    Compose hands every service a ``postgres://`` URL. SQLAlchemy needs the driver
    named, so rather than keeping a second spelling of the same connection string in
    the environment, it is rewritten here.
    """
    for prefix in ("postgresql+asyncpg://", "postgres+asyncpg://"):
        if dsn.startswith(prefix):
            return dsn.replace("postgres+asyncpg://", "postgresql+asyncpg://", 1)
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            return "postgresql+asyncpg://" + dsn[len(prefix) :]
    return dsn


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )


class UnitOfWork:
    """Runs a callable inside one transaction."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[AsyncSession]:
        """Yield a session inside a transaction, committing on a clean exit.

        Anything raised rolls the whole thing back, which is what makes it safe for a
        use case to write its state change and its outbox row without thinking about
        ordering.

        Nesting reuses the outer transaction rather than opening a second one. Without
        that, a use case calling another would commit half the work early and lose the
        all-or-nothing guarantee the outbox depends on.
        """
        existing = session_var.get()
        if existing is not None:
            yield existing
            return

        async with self._sessions() as session:
            token = session_var.set(session)
            try:
                async with session.begin():
                    yield session
            finally:
                session_var.reset(token)

    @asynccontextmanager
    async def read(self) -> AsyncIterator[AsyncSession]:
        """Yield a session for reads, with no write transaction.

        Queries need a session as much as commands do — the repositories take it from
        ``current_session`` either way — but they must not open a read-write transaction. One
        held open across a slow query keeps a connection and blocks vacuum for no reason, and
        a use case that only reads should not be able to commit by accident.

        Nesting inside ``begin()`` reuses the outer session, so a command that reads before it
        writes sees its own uncommitted work.
        """
        existing = session_var.get()
        if existing is not None:
            yield existing
            return

        async with self._sessions() as session:
            token = session_var.set(session)
            try:
                yield session
            finally:
                session_var.reset(token)
                # Any implicit transaction SQLAlchemy opened for the SELECTs is discarded
                # rather than committed. Nothing here was supposed to write.
                await session.rollback()


def strip_asyncpg_dsn(dsn: str) -> str:
    """Return a DSN for asyncpg's own connect(), which rejects SQLAlchemy's prefix.

    Used by the migration runner, which talks to the database before the ORM exists.
    """
    normalised = _normalise_dsn(dsn)
    return normalised.replace("postgresql+asyncpg://", "postgresql://", 1)
