"""
main.py — Entry point for the Polymarket BTC 15m price ingestion + arb execution service.

Startup sequence:
  1. Hit the Gamma API to find the currently active BTC 15m market token IDs.
  2. Initialise Polymarket auth (derive L2 HMAC credentials from wallet).
  3. Connect to the Polymarket CLOB WebSocket.
  4. Subscribe to both outcomes (Up / Down) with custom_feature_enabled for
     the fastest best_bid_ask events.
  5. Arb executor monitors every tick; when ask_UP + ask_DOWN + fees < 1.0,
     it signs and POSTs both legs concurrently via FAK orders.

Run:
    pip install -r requirements.txt
    POLYMARKET_PRIVATE_KEY=0x... python main.py
"""

import asyncio
import logging
import os
import sys
import time

import aiohttp

from market import fetch_active_btc_market, BtcMarket
from ws_ingestor import BtcMarketIngestor, PriceEvent
from auth import PolyAuth
from db import SupabaseWriter
from executor import ArbExecutor, Mode
import server

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Price callback — replace this with your trading logic
# ---------------------------------------------------------------------------

# Track time of last print per label to avoid flooding the terminal
_last_printed: dict[str, float] = {}
_PRINT_THROTTLE_S = 0.05  # print at most every 50ms per token


async def on_price(event: PriceEvent) -> None:
    """
    Called for every normalised price event from the WebSocket.

    This stub just pretty-prints the event.  For a trading system you would:
      - Feed into a feature store / ring buffer
      - Compute signals (spread, momentum, imbalance)
      - Send orders via the CLOB REST API
    """
    now = time.monotonic()
    last = _last_printed.get(event.label, 0.0)
    if now - last < _PRINT_THROTTLE_S and event.event_type not in ("last_trade_price", "book"):
        return  # throttle repetitive best_bid_ask / price_change noise

    _last_printed[event.label] = now

    # Build a compact display line
    mid_str = f"{event.mid:.4f}" if event.mid is not None else "  ----"
    bid_str = f"{event.best_bid:.4f}" if event.best_bid is not None else "  ----"
    ask_str = f"{event.best_ask:.4f}" if event.best_ask is not None else "  ----"

    trade_str = ""
    if event.event_type == "last_trade_price" and event.last_trade is not None:
        trade_str = f"  TRADE {event.last_trade_side} {event.last_trade:.4f} x {event.last_trade_size:.1f}"

    latency_us = ""
    if event.exchange_ts:
        # exchange_ts is in milliseconds; recv_ns is monotonic — use wall clock diff
        pass  # TODO: sync wall clock with monotonic for accurate latency

    logger.info(
        "[%-4s] %-16s bid=%-8s ask=%-8s mid=%s%s",
        event.label,
        event.event_type,
        bid_str,
        ask_str,
        mid_str,
        trade_str,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    logger.info("=== Polymarket BTC 15m Price Ingestor ===")

    # 1. Open a long-lived HTTP session (kept alive for market rotation)
    session = aiohttp.ClientSession()

    # 2. Discover the active market
    logger.info("Looking up active BTC 15m market…")
    market: BtcMarket | None = await fetch_active_btc_market(session)

    if market is None:
        logger.error(
            "Could not find an active BTC 15m market. "
            "The market may be between windows (settling / not yet open). "
            "Retrying in 30s…"
        )
        await session.close()
        await asyncio.sleep(30)
        sys.exit(1)

    logger.info("Market  : %s", market.question)
    logger.info("Slug    : %s", market.slug)
    logger.info("Up  ID  : %s…", market.up_token_id[:20])
    logger.info("Down ID : %s…", market.down_token_id[:20])

    # 3. Initialise Polymarket auth (one-time, derives L2 credentials)
    auth = PolyAuth()  # reads POLYMARKET_PRIVATE_KEY from env
    auth.setup()

    # 4. Start the web UI (shares this event loop — returns immediately)
    web_port = int(os.environ.get("PORT", "8765"))
    await server.start_server(host="0.0.0.0", port=web_port)

    # 5. Create a dedicated HTTP session for order submission (persistent connections)
    order_connector = aiohttp.TCPConnector(
        limit=4,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    order_session = aiohttp.ClientSession(connector=order_connector)

    # 6. Create the ingestor, inject session, register callbacks
    ingestor = BtcMarketIngestor(
        up_token_id=market.up_token_id,
        down_token_id=market.down_token_id,
    )
    ingestor.set_http_session(session)    # enables auto market rotation

    # 7. Create Supabase writer (reads SUPABASE_URL + SUPABASE_KEY from env)
    db_writer = SupabaseWriter()
    if db_writer.enabled:
        db_writer.set_session(order_session)

    # 8. Create the arb executor
    mode = Mode.LIVE if os.environ.get("POLY_LIVE") == "1" else Mode.DRY_RUN
    proxy_url = os.environ.get("PROXY_URL", "")
    executor = ArbExecutor(
        auth=auth,
        ingestor=ingestor,
        http_session=order_session,
        mode=mode,
        max_size=float(os.environ.get("POLY_MAX_SIZE", "20")),
        min_edge_bps=float(os.environ.get("POLY_MIN_EDGE_BPS", "0")),
        cooldown_s=float(os.environ.get("POLY_COOLDOWN_S", "2.0")),
        proxy_url=proxy_url,
        db=db_writer,
    )
    logger.info("Executor mode: %s | Proxy: %s", mode.name, proxy_url.split("@")[-1] if proxy_url else "NONE")

    # Wire executor + wallet into the dashboard (read-only, no hot-path impact)
    server.set_executor(executor)
    server.set_wallet_address(auth.address)
    server.set_http_session(order_session)
    server.set_polygon_rpc_url(os.environ.get("POLYGON_RPC_URL", ""))
    server.set_db_writer(db_writer)

    # Register callbacks — executor first for lowest latency
    ingestor.on_price(executor)           # arb execution (fastest)
    ingestor.on_price(on_price)           # stdout logger
    ingestor.on_price(server.broadcast)   # browser WebSocket fan-out

    # 8. Stream forever (ctrl-c to stop)
    logger.info("Starting WebSocket stream — press Ctrl-C to stop.")
    try:
        await ingestor.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        await ingestor.stop()
        await order_session.close()
        await session.close()
        await server.stop_server()
        logger.info("Executor summary: %s", executor.summary())
        logger.info("Ingestor stopped.")


if __name__ == "__main__":
    asyncio.run(main())
