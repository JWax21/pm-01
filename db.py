"""
db.py — Fire-and-forget Supabase persistence for arb attempts.

Uses raw aiohttp POST to the Supabase PostgREST API.
Non-blocking: all writes run as background asyncio tasks.
Failures are logged and silently dropped (in-memory history is primary).
"""

import asyncio
import json
import logging
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


class SupabaseWriter:
    """
    Async writer for arb attempt records to Supabase.

    Usage:
        writer = SupabaseWriter()       # reads env vars
        writer.set_session(session)     # inject shared aiohttp session
        writer.fire(row_dict)           # fire-and-forget background write
    """

    def __init__(self) -> None:
        self._url = os.environ.get("SUPABASE_URL", "").rstrip("/")
        self._key = os.environ.get("SUPABASE_KEY", "")
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = bool(self._url and self._key)

        if self._enabled:
            logger.info("Supabase persistence enabled: %s", self._url)
        else:
            logger.info("Supabase persistence disabled (SUPABASE_URL/SUPABASE_KEY not set)")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    def fire(self, row: dict) -> None:
        """Fire-and-forget write. Safe to call from any async context."""
        if not self._enabled:
            return
        asyncio.create_task(self._write(row))

    async def _write(self, row: dict) -> None:
        """POST a single row to Supabase PostgREST. Logs errors, never raises."""
        if self._session is None or self._session.closed:
            logger.warning("DB write skipped: no active aiohttp session")
            return

        url = f"{self._url}/rest/v1/pm_01_arb_attempts"
        headers = {
            "apikey": self._key,
            "Authorization": f"Bearer {self._key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

        try:
            async with self._session.post(
                url,
                data=json.dumps(row, separators=(",", ":"), default=str),
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    logger.warning(
                        "Supabase write failed (HTTP %d): %s",
                        resp.status,
                        body[:200],
                    )
        except Exception as exc:
            logger.warning("Supabase write error: %s", exc)
