"""
market.py — Discover the active BTC 15m market token IDs via the Gamma API.

The BTC Up/Down 15m series creates a new market every 15 minutes.
Slugs follow the pattern: btc-updown-15m-{unix_timestamp}
where the timestamp is the start of the current 15-minute window (floor to 900s).

This module resolves the current window's token IDs with a direct slug lookup,
falling back to the previous window if the new one isn't live yet.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
WINDOW_SECONDS = 900  # 15 minutes


@dataclass
class BtcMarket:
    condition_id: str
    slug: str
    question: str
    end_date: str
    up_token_id: str
    down_token_id: str

    @property
    def token_ids(self) -> list[str]:
        return [self.up_token_id, self.down_token_id]


def _current_window_ts() -> int:
    """Unix timestamp floored to the current 15-minute boundary."""
    return (int(time.time()) // WINDOW_SECONDS) * WINDOW_SECONDS


async def fetch_active_btc_market(session: aiohttp.ClientSession) -> Optional[BtcMarket]:
    """
    Return the currently active BTC 15m market.

    Tries the current 15-minute window first, then falls back up to 3 windows
    back in case we're between windows or the market hasn't opened yet.
    Also tries the next window forward for markets opened slightly early.
    """
    base_ts = _current_window_ts()

    # Try current window, one ahead, and two behind
    offsets = [0, WINDOW_SECONDS, -WINDOW_SECONDS, -2 * WINDOW_SECONDS]
    for offset in offsets:
        ts = base_ts + offset
        market = await _fetch_by_slug(session, ts)
        if market:
            logger.info("Found active BTC 15m market at window ts=%d (%s)", ts, market.question)
            return market

    logger.error(
        "No active BTC 15m market found near window ts=%d. "
        "The series may be paused or the slug pattern changed.",
        base_ts,
    )
    return None


async def _fetch_by_slug(session: aiohttp.ClientSession, window_ts: int) -> Optional[BtcMarket]:
    """
    Fetch the event for a specific 15-minute window timestamp.
    Slug format: btc-updown-15m-{window_ts}
    """
    slug = f"btc-updown-15m-{window_ts}"
    url = f"{GAMMA_API}/events"
    params = {"slug": slug}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            resp.raise_for_status()
            events: list = await resp.json()
    except Exception as exc:
        logger.debug("Slug lookup %s failed: %s", slug, exc)
        return None

    if not events:
        logger.debug("No event found for slug=%s", slug)
        return None

    event = events[0]

    # Only return markets that are active and not yet closed
    if not event.get("active") or event.get("closed"):
        logger.debug("Market %s is not active or is closed", slug)
        return None

    return _parse_event(event)


def _parse_event(event: dict) -> Optional[BtcMarket]:
    """
    Extract token IDs from a Gamma API event object.

    Note: clobTokenIds is returned as a JSON *string* (e.g. '["123", "456"]'),
    not a Python list — we must json.loads() it.
    """
    try:
        markets = event.get("markets") or []
        if not markets:
            logger.debug("Event has no markets: %s", event.get("slug"))
            return None

        mkt = markets[0]
        raw_token_ids = mkt.get("clobTokenIds")

        if raw_token_ids is None:
            logger.debug("clobTokenIds missing in market %s", mkt.get("conditionId"))
            return None

        # clobTokenIds may be a JSON string or already a list
        if isinstance(raw_token_ids, str):
            token_ids: list[str] = json.loads(raw_token_ids)
        else:
            token_ids = list(raw_token_ids)

        if len(token_ids) < 2:
            logger.debug("Expected 2 token IDs, got %d: %s", len(token_ids), token_ids)
            return None

        return BtcMarket(
            condition_id=mkt.get("conditionId", ""),
            slug=event.get("slug", ""),
            question=event.get("title", "BTC 15m"),
            end_date=event.get("endDate", ""),
            up_token_id=token_ids[0],
            down_token_id=token_ids[1],
        )

    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        logger.debug("Failed to parse event: %s — %s", exc, event.get("slug"))
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    async def _main():
        async with aiohttp.ClientSession() as session:
            market = await fetch_active_btc_market(session)
            if market:
                print(f"\nActive market : {market.question}")
                print(f"Slug          : {market.slug}")
                print(f"End date      : {market.end_date}")
                print(f"Condition ID  : {market.condition_id}")
                print(f"Up  token     : {market.up_token_id}")
                print(f"Down token    : {market.down_token_id}")
            else:
                print("No active BTC 15m market found.")

    asyncio.run(_main())
