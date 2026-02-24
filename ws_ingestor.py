"""
ws_ingestor.py — High-speed Polymarket WebSocket price ingestion service.

Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market, subscribes
to BTC 15m token IDs, and streams price events with sub-millisecond timestamps.

Event priority (fastest signal first):
  1. best_bid_ask  — top-of-book shift (requires custom_feature_enabled)
  2. last_trade_price — confirmed fill
  3. price_change  — order placed/cancelled (full depth delta)
  4. book          — full snapshot (on connect + after each trade)
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable, Awaitable
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10       # seconds — Polymarket requires PING every 10s
RECONNECT_DELAY = 1.0    # seconds — base delay before reconnect
MAX_RECONNECT_DELAY = 30 # seconds — cap on exponential back-off


# ---------------------------------------------------------------------------
# Price state — maintained locally from stream events
# ---------------------------------------------------------------------------

@dataclass
class TokenPrice:
    """Current best price state for a single token (outcome)."""
    token_id: str
    label: str  # "Up" or "Down"

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade: Optional[float] = None
    last_trade_side: Optional[str] = None  # "BUY" or "SELL"
    last_trade_size: Optional[float] = None
    mid: Optional[float] = None

    updated_at_ns: int = 0  # monotonic nanoseconds for latency tracking

    def update_bba(self, bid: float, ask: float, ts_ns: int) -> None:
        self.best_bid = bid
        self.best_ask = ask
        self.mid = (bid + ask) / 2 if bid and ask else None
        self.updated_at_ns = ts_ns

    def update_trade(self, price: float, side: str, size: float, ts_ns: int) -> None:
        self.last_trade = price
        self.last_trade_side = side
        self.last_trade_size = size
        self.updated_at_ns = ts_ns

    def __repr__(self) -> str:
        return (
            f"<{self.label} bid={self.best_bid} ask={self.best_ask} "
            f"mid={self.mid:.4f} last={self.last_trade}>"
            if self.mid is not None
            else f"<{self.label} (no data yet)>"
        )


@dataclass
class MarketState:
    """Aggregated state for both outcomes of the BTC 15m market."""
    up: TokenPrice
    down: TokenPrice
    events_received: int = 0
    connected_at: float = field(default_factory=time.monotonic)

    def by_token(self, token_id: str) -> Optional[TokenPrice]:
        if token_id == self.up.token_id:
            return self.up
        if token_id == self.down.token_id:
            return self.down
        return None


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

PriceCallback = Callable[["PriceEvent"], Awaitable[None]]


@dataclass
class PriceEvent:
    """Normalised price event emitted to downstream consumers."""
    event_type: str          # "best_bid_ask" | "last_trade_price" | "price_change" | "book"
    token_id: str
    label: str               # "Up" | "Down"
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid: Optional[float]
    last_trade: Optional[float]
    last_trade_side: Optional[str]
    last_trade_size: Optional[float]
    raw: dict                # full original message for anything not normalised
    recv_ns: int             # monotonic nanoseconds at message receipt
    exchange_ts: Optional[int] = None  # milliseconds from exchange timestamp field


# ---------------------------------------------------------------------------
# Ingestor
# ---------------------------------------------------------------------------

class BtcMarketIngestor:
    """
    Subscribes to Polymarket's CLOB WebSocket for a BTC 15m market and
    emits normalised PriceEvent objects to registered callbacks.

    Usage::

        ingestor = BtcMarketIngestor(up_token_id="...", down_token_id="...")
        ingestor.on_price(my_async_callback)
        await ingestor.run()
    """

    def __init__(self, up_token_id: str, down_token_id: str) -> None:
        self.state = MarketState(
            up=TokenPrice(token_id=up_token_id, label="Up"),
            down=TokenPrice(token_id=down_token_id, label="Down"),
        )
        self._callbacks: list[PriceCallback] = []
        self._running = False
        self._ws = None                                          # set during _connect_and_stream
        self._http_session: Optional[aiohttp.ClientSession] = None  # injected by caller

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_price(self, callback: PriceCallback) -> None:
        """Register an async callback to receive PriceEvent objects."""
        self._callbacks.append(callback)

    def set_http_session(self, session: aiohttp.ClientSession) -> None:
        """Inject a long-lived aiohttp session for use during market rotation."""
        self._http_session = session

    async def run(self) -> None:
        """
        Connect and stream forever, reconnecting on failure.
        Cancel the task to stop cleanly.
        """
        self._running = True
        delay = RECONNECT_DELAY
        attempt = 0

        while self._running:
            attempt += 1
            try:
                logger.info("Connecting to %s (attempt %d)", WS_URL, attempt)
                await self._connect_and_stream()
                delay = RECONNECT_DELAY  # reset on clean disconnect
            except asyncio.CancelledError:
                logger.info("Ingestor cancelled — stopping.")
                self._running = False
                return
            except Exception as exc:
                logger.warning("Connection error: %s — reconnecting in %.1fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, MAX_RECONNECT_DELAY)

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(
            WS_URL,
            ping_interval=None,   # we handle PING ourselves (Polymarket uses plain text "PING")
            max_size=2**20,       # 1 MB max message size
            open_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            logger.info("WebSocket connected.")
            await self._subscribe(ws)

            ping_task = asyncio.create_task(self._ping_loop(ws))
            try:
                async for raw_msg in ws:
                    recv_ns = time.monotonic_ns()
                    await self._handle_message(raw_msg, recv_ns)
            except ConnectionClosed as exc:
                logger.info("Connection closed: %s", exc)
            finally:
                self._ws = None
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _subscribe(self, ws) -> None:
        sub = {
            "assets_ids": self.state.up.token_id + "," + self.state.down.token_id,
            "type": "market",
            "custom_feature_enabled": True,
        }
        # assets_ids must be a list per the API spec
        sub["assets_ids"] = [self.state.up.token_id, self.state.down.token_id]
        await ws.send(json.dumps(sub))
        logger.info(
            "Subscribed: up=%s... down=%s...",
            self.state.up.token_id[:12],
            self.state.down.token_id[:12],
        )

    async def _ping_loop(self, ws) -> None:
        """Send a plain-text PING every PING_INTERVAL seconds."""
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                await ws.send("PING")
                logger.debug("PING sent")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Ping loop ended: %s", exc)

    async def _handle_message(self, raw_msg: str | bytes, recv_ns: int) -> None:
        if raw_msg == "PONG":
            logger.debug("PONG received")
            return

        try:
            data: dict | list = json.loads(raw_msg)
        except json.JSONDecodeError:
            logger.debug("Non-JSON message: %r", raw_msg)
            return

        # The market channel sometimes wraps events in a list
        if isinstance(data, list):
            for item in data:
                await self._dispatch(item, recv_ns)
        else:
            await self._dispatch(data, recv_ns)

    async def _dispatch(self, msg: dict, recv_ns: int) -> None:
        self.state.events_received += 1
        event_type = msg.get("event_type", "")

        if event_type == "best_bid_ask":
            await self._on_best_bid_ask(msg, recv_ns)
        elif event_type == "last_trade_price":
            await self._on_last_trade(msg, recv_ns)
        elif event_type == "price_change":
            await self._on_price_change(msg, recv_ns)
        elif event_type == "book":
            await self._on_book(msg, recv_ns)
        elif event_type == "market_resolved":
            logger.info("Market resolved — scheduling rotation")
            asyncio.create_task(self._rotate_market())
        elif event_type == "tick_size_change":
            await self._on_tick_size(msg, recv_ns)
        elif event_type == "new_market":
            logger.info("Market event: %s", event_type)
        else:
            logger.debug("Unknown event_type=%r: %s", event_type, msg)

    # ------------------------------------------------------------------
    # Market rotation
    # ------------------------------------------------------------------

    async def _rotate_market(self) -> None:
        """
        Called (as a background task) when market_resolved fires.
        Polls the Gamma API for the next BTC 15m market, then sends
        unsubscribe/subscribe to the live WebSocket without reconnecting.
        """
        # Import here to avoid a circular import at module level
        from market import fetch_active_btc_market

        if self._http_session is None or self._ws is None:
            logger.warning("Cannot rotate market: session or ws unavailable")
            return

        old_up   = self.state.up.token_id
        old_down = self.state.down.token_id

        # Wait briefly for Polymarket to publish the new market
        await asyncio.sleep(3)

        new_market = None
        for attempt in range(12):           # retry every 5 s for up to 60 s
            candidate = await fetch_active_btc_market(self._http_session)
            if candidate and candidate.up_token_id != old_up:
                new_market = candidate
                break
            logger.info("Waiting for new BTC 15m market… (attempt %d/12)", attempt + 1)
            await asyncio.sleep(5)

        if new_market is None:
            logger.error("Market rotation failed: no new market found after 60 s")
            return

        if self._ws is None:
            logger.warning("WebSocket closed before rotation could complete")
            return

        # Unsubscribe old tokens
        await self._ws.send(json.dumps({
            "assets_ids": [old_up, old_down],
            "operation": "unsubscribe",
        }))

        # Swap state
        self.state = MarketState(
            up=TokenPrice(token_id=new_market.up_token_id, label="Up"),
            down=TokenPrice(token_id=new_market.down_token_id, label="Down"),
        )

        # Subscribe new tokens
        await self._ws.send(json.dumps({
            "assets_ids": [new_market.up_token_id, new_market.down_token_id],
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }))

        logger.info(
            "Rotated to new market: %s (up=%s… down=%s…)",
            new_market.question,
            new_market.up_token_id[:12],
            new_market.down_token_id[:12],
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_best_bid_ask(self, msg: dict, recv_ns: int) -> None:
        """Fastest signal — top-of-book price shift."""
        token_id = msg.get("asset_id", "")
        token = self.state.by_token(token_id)
        if token is None:
            return

        bid = _to_float(msg.get("best_bid"))
        ask = _to_float(msg.get("best_ask"))
        if bid is None or ask is None:
            return

        token.update_bba(bid, ask, recv_ns)

        event = PriceEvent(
            event_type="best_bid_ask",
            token_id=token_id,
            label=token.label,
            best_bid=bid,
            best_ask=ask,
            mid=token.mid,
            last_trade=token.last_trade,
            last_trade_side=token.last_trade_side,
            last_trade_size=token.last_trade_size,
            raw=msg,
            recv_ns=recv_ns,
            exchange_ts=_to_int(msg.get("timestamp")),
        )
        await self._emit(event)

    async def _on_last_trade(self, msg: dict, recv_ns: int) -> None:
        """Confirmed fill — actual trade executed."""
        token_id = msg.get("asset_id", "")
        token = self.state.by_token(token_id)
        if token is None:
            return

        price = _to_float(msg.get("price"))
        side = msg.get("side", "")
        size = _to_float(msg.get("size"))
        if price is None:
            return

        token.update_trade(price, side, size or 0.0, recv_ns)

        event = PriceEvent(
            event_type="last_trade_price",
            token_id=token_id,
            label=token.label,
            best_bid=token.best_bid,
            best_ask=token.best_ask,
            mid=token.mid,
            last_trade=price,
            last_trade_side=side,
            last_trade_size=size,
            raw=msg,
            recv_ns=recv_ns,
            exchange_ts=_to_int(msg.get("timestamp")),
        )
        await self._emit(event)

    async def _on_price_change(self, msg: dict, recv_ns: int) -> None:
        """Order book depth delta — order placed or cancelled."""
        changes = msg.get("price_changes")
        if not changes:
            # Flat schema (migration variant)
            changes = [msg]

        exchange_ts = _to_int(msg.get("timestamp"))

        for change in changes:
            token_id = change.get("asset_id", "")
            token = self.state.by_token(token_id)
            if token is None:
                continue

            # Update best bid/ask if provided in the change
            bid = _to_float(change.get("best_bid"))
            ask = _to_float(change.get("best_ask"))
            if bid is not None and ask is not None:
                token.update_bba(bid, ask, recv_ns)

            event = PriceEvent(
                event_type="price_change",
                token_id=token_id,
                label=token.label,
                best_bid=token.best_bid,
                best_ask=token.best_ask,
                mid=token.mid,
                last_trade=token.last_trade,
                last_trade_side=token.last_trade_side,
                last_trade_size=token.last_trade_size,
                raw=change,
                recv_ns=recv_ns,
                exchange_ts=exchange_ts,
            )
            await self._emit(event)

    async def _on_book(self, msg: dict, recv_ns: int) -> None:
        """Full order book snapshot — emitted on connect and after each trade."""
        token_id = msg.get("asset_id", "")
        token = self.state.by_token(token_id)
        if token is None:
            return

        bids: list[dict] = msg.get("bids", [])
        asks: list[dict] = msg.get("asks", [])

        best_bid = max((_to_float(b["price"]) for b in bids if b.get("price")), default=None)
        best_ask = min((_to_float(a["price"]) for a in asks if a.get("price")), default=None)

        if best_bid is not None and best_ask is not None:
            token.update_bba(best_bid, best_ask, recv_ns)

        event = PriceEvent(
            event_type="book",
            token_id=token_id,
            label=token.label,
            best_bid=token.best_bid,
            best_ask=token.best_ask,
            mid=token.mid,
            last_trade=token.last_trade,
            last_trade_side=token.last_trade_side,
            last_trade_size=token.last_trade_size,
            raw=msg,
            recv_ns=recv_ns,
            exchange_ts=_to_int(msg.get("timestamp")),
        )
        await self._emit(event)

    async def _on_tick_size(self, msg: dict, recv_ns: int) -> None:
        """Tick size change notification — rare market parameter update."""
        token_id = msg.get("asset_id", "")
        token = self.state.by_token(token_id)
        label = token.label if token else "?"

        event = PriceEvent(
            event_type="tick_size",
            token_id=token_id,
            label=label,
            best_bid=None,
            best_ask=None,
            mid=None,
            last_trade=None,
            last_trade_side=None,
            last_trade_size=None,
            raw=msg,
            recv_ns=recv_ns,
            exchange_ts=_to_int(msg.get("timestamp")),
        )
        await self._emit(event)

    async def _emit(self, event: PriceEvent) -> None:
        for cb in self._callbacks:
            try:
                await cb(event)
            except Exception as exc:
                logger.error("Callback error: %s", exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_int(val: Any) -> Optional[int]:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None
