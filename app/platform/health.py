"""Readiness checks.

A check is either critical or it is not, and saying which is the whole value of this
module. Postgres down means this service cannot do its job: DOWN, stop sending
traffic. Kafka down means events queue in the outbox and drain later: DEGRADED, keep
serving. Marking a non-critical dependency critical is how one broker hiccup takes a
whole platform offline.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

Check = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class Probe:
    name: str
    check: Check
    critical: bool = True
    timeout: float = 3.0


class Registry:
    def __init__(self, *, service: str, version: str) -> None:
        self._service = service
        self._version = version
        self._probes: list[Probe] = []

    def add(self, name: str, check: Check, *, critical: bool = True, timeout: float = 3.0) -> None:
        self._probes.append(Probe(name, check, critical, timeout))

    async def report(self) -> dict:
        results: dict[str, dict[str, str]] = {}
        status = "UP"

        async def run(probe: Probe) -> None:
            nonlocal status
            try:
                await asyncio.wait_for(probe.check(), timeout=probe.timeout)
            except TimeoutError:
                results[probe.name] = {
                    "status": "DOWN",
                    "error": f"timed out after {probe.timeout}s",
                }
            except Exception as exc:
                results[probe.name] = {"status": "DOWN", "error": str(exc)[:200]}
            else:
                results[probe.name] = {"status": "UP"}

        await asyncio.gather(*(run(p) for p in self._probes))

        for probe in self._probes:
            if results[probe.name]["status"] == "DOWN":
                if probe.critical:
                    status = "DOWN"
                elif status == "UP":
                    status = "DEGRADED"

        return {
            "status": status,
            "service": self._service,
            "version": self._version,
            "checks": results,
        }
