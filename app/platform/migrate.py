"""Migrations: plain SQL files, applied in order, once.

No migration framework. The files are numbered, they run in that order, and a
checksum is stored so an already-applied file that changed on disk stops the boot
instead of leaving two environments quietly different.

An advisory lock makes it safe for several replicas to start at the same moment:
they all try, one wins, the rest wait and then find nothing to do.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import asyncpg

LOCK_KEY = 0x41524341  # "ARCA"

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

logger = logging.getLogger(__name__)


class MigrationError(RuntimeError):
    """A migration that cannot be applied, or one that changed after being applied."""


def _discover(directory: Path) -> list[tuple[str, str]]:
    files = sorted(p for p in directory.glob("*.sql") if p.is_file())
    if not files:
        raise MigrationError(f"no .sql files found in {directory}")
    return [(p.stem, p.read_text(encoding="utf-8")) for p in files]


async def run(dsn: str, directory: Path) -> int:
    """Apply every pending migration. Returns how many ran."""
    migrations = _discover(directory)

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(_BOOTSTRAP)
        await conn.execute("SELECT pg_advisory_lock($1)", LOCK_KEY)
        try:
            rows = await conn.fetch("SELECT version, checksum FROM schema_migrations")
            applied = {r["version"]: r["checksum"] for r in rows}

            ran = 0
            for version, sql in migrations:
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                if version in applied:
                    if applied[version] != checksum:
                        raise MigrationError(
                            f"migration {version} has already been applied but its "
                            f"contents have changed; add a new migration instead of "
                            f"editing an applied one"
                        )
                    continue

                # Each migration is its own transaction: a failure half way through
                # the set leaves the ones before it applied and recorded, so the
                # retry starts from the one that broke.
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)",
                        version,
                        checksum,
                    )
                logger.info("applied migration %s", version)
                ran += 1
            return ran
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", LOCK_KEY)
    finally:
        await conn.close()
