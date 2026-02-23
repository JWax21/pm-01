"""
server.py — aiohttp HTTP + WebSocket server for the Polymarket live feed UI.

Runs inside the existing asyncio event loop (no threads, no separate process).
Browser clients connect to ws://localhost:8765/ws and receive every PriceEvent
serialised as JSON in real time.
"""

import asyncio
import dataclasses
import json
import logging
import time as _time
from typing import TYPE_CHECKING, Optional

import aiohttp
from aiohttp import web

from ws_ingestor import PriceEvent

if TYPE_CHECKING:
    from executor import ArbExecutor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client registry
# ---------------------------------------------------------------------------

# All currently connected browser WebSocket clients.
CLIENTS: set[web.WebSocketResponse] = set()


async def broadcast(event: PriceEvent) -> None:
    """
    Registered as a PriceEvent callback on the ingestor.
    Serialises the event and fans it out to every connected browser client.
    Dead/slow clients are removed from the registry after the sweep.
    """
    if not CLIENTS:
        return

    payload = json.dumps(dataclasses.asdict(event))
    dead: set[web.WebSocketResponse] = set()

    for ws in CLIENTS:
        try:
            await ws.send_str(payload)
        except Exception:
            dead.add(ws)

    # Remove dead clients after iteration to avoid mutating set mid-loop.
    # Use difference_update (in-place) rather than -= to avoid Python treating
    # CLIENTS as a local variable due to the augmented assignment.
    CLIENTS.difference_update(dead)


# ---------------------------------------------------------------------------
# Dashboard state (set once at startup, read-only thereafter)
# ---------------------------------------------------------------------------

_executor: Optional["ArbExecutor"] = None
_wallet_address: str = ""
_http_session: Optional[aiohttp.ClientSession] = None


def set_executor(executor: "ArbExecutor") -> None:
    global _executor
    _executor = executor


def set_wallet_address(address: str) -> None:
    global _wallet_address
    _wallet_address = address


def set_http_session(session: aiohttp.ClientSession) -> None:
    global _http_session
    _http_session = session


_polygon_rpc_url: str = ""


def set_polygon_rpc_url(url: str) -> None:
    global _polygon_rpc_url
    _polygon_rpc_url = url


# ---------------------------------------------------------------------------
# Dashboard API endpoints
# ---------------------------------------------------------------------------

_actual_trades_cache: dict = {"data": [], "fetched_at": 0.0}
_CACHE_TTL_S = 15.0

_balance_cache: dict = {"data": {}, "fetched_at": 0.0}
_BALANCE_TTL_S = 30.0
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_E_DECIMALS = 6


async def api_potential_trades(request: web.Request) -> web.Response:
    """Return executor arb history + stats as JSON. Pure in-memory read."""
    if _executor is None:
        return web.json_response({"trades": [], "stats": {}})

    trades = []
    for attempt in _executor.history:
        # Determine status
        if attempt.up_result is None and attempt.down_result is None:
            status = "dry_run"
        elif attempt.both_filled:
            status = "both_filled"
        elif attempt.one_leg_only:
            status = "one_leg"
        elif (attempt.up_result and not attempt.up_result.success
              and attempt.down_result and not attempt.down_result.success):
            status = "neither_filled"
        else:
            status = "error"

        trades.append({
            "timestamp": attempt.timestamp,
            "up_ask": attempt.up_ask,
            "down_ask": attempt.down_ask,
            "total": round(attempt.up_ask + attempt.down_ask, 6),
            "edge_bps": attempt.edge_bps,
            "size": attempt.size,
            "status": status,
            "up_result": dataclasses.asdict(attempt.up_result) if attempt.up_result else None,
            "down_result": dataclasses.asdict(attempt.down_result) if attempt.down_result else None,
            "generation": attempt.generation,
            "mode": attempt.mode,
            "sign_duration_ms": attempt.sign_duration_ms,
            "total_duration_ms": attempt.total_duration_ms,
        })

    stats = dataclasses.asdict(_executor.stats)

    return web.json_response({
        "trades": list(reversed(trades)),  # newest first
        "stats": stats,
    })


async def api_actual_trades(request: web.Request) -> web.Response:
    """Proxy Polymarket Data API /activity endpoint with server-side cache."""
    now = _time.monotonic()

    if now - _actual_trades_cache["fetched_at"] < _CACHE_TTL_S:
        return web.json_response(_actual_trades_cache["data"])

    if not _wallet_address:
        return web.json_response([])

    session = _http_session or aiohttp.ClientSession()
    close_session = _http_session is None

    try:
        url = (
            f"https://data-api.polymarket.com/activity"
            f"?user={_wallet_address}&type=TRADE&limit=100"
        )
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                _actual_trades_cache["data"] = data
                _actual_trades_cache["fetched_at"] = now
                return web.json_response(data)
            else:
                return web.json_response(
                    {"error": f"Data API returned {resp.status}"},
                    status=502,
                )
    except Exception as exc:
        logger.warning("Data API fetch failed: %s", exc)
        if _actual_trades_cache["data"]:
            return web.json_response(_actual_trades_cache["data"])
        return web.json_response({"error": str(exc)}, status=502)
    finally:
        if close_session:
            await session.close()


async def _rpc_call(session: aiohttp.ClientSession, url: str, payload: dict) -> dict:
    """Make a single JSON-RPC call."""
    async with session.post(
        url, json=payload, timeout=aiohttp.ClientTimeout(total=5)
    ) as resp:
        return await resp.json()


async def api_wallet_balance(request: web.Request) -> web.Response:
    """Return wallet USDC.e and POL balances via Polygon RPC."""
    now = _time.monotonic()

    if now - _balance_cache["fetched_at"] < _BALANCE_TTL_S and _balance_cache["data"]:
        return web.json_response(_balance_cache["data"])

    if not _wallet_address or not _polygon_rpc_url:
        return web.json_response({"usdc_e": None, "pol": None, "wallet": _wallet_address})

    session = _http_session or aiohttp.ClientSession()
    close_session = _http_session is None

    try:
        addr_padded = _wallet_address.lower().replace("0x", "").zfill(64)

        usdc_payload = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": USDC_E_ADDRESS, "data": "0x70a08231" + addr_padded}, "latest"],
        }
        pol_payload = {
            "jsonrpc": "2.0", "id": 2, "method": "eth_getBalance",
            "params": [_wallet_address, "latest"],
        }

        usdc_data, pol_data = await asyncio.gather(
            _rpc_call(session, _polygon_rpc_url, usdc_payload),
            _rpc_call(session, _polygon_rpc_url, pol_payload),
        )

        usdc_raw = int(usdc_data.get("result", "0x0"), 16)
        usdc_balance = usdc_raw / (10 ** USDC_E_DECIMALS)

        pol_raw = int(pol_data.get("result", "0x0"), 16)
        pol_balance = pol_raw / (10 ** 18)

        result = {
            "usdc_e": round(usdc_balance, 6),
            "pol": round(pol_balance, 6),
            "wallet": _wallet_address,
        }

        _balance_cache["data"] = result
        _balance_cache["fetched_at"] = now
        return web.json_response(result)

    except Exception as exc:
        logger.warning("Balance fetch failed: %s", exc)
        if _balance_cache["data"]:
            return web.json_response(_balance_cache["data"])
        return web.json_response({"error": str(exc), "usdc_e": None, "pol": None}, status=502)
    finally:
        if close_session:
            await session.close()


async def api_debug_ip(request: web.Request) -> web.Response:
    """Diagnostic: show outbound IP and Polymarket geoblock status (direct + proxy)."""
    session = _http_session or aiohttp.ClientSession()
    close_session = _http_session is None
    result: dict = {}
    proxy_url = _executor.proxy_url if _executor else None
    timeout = aiohttp.ClientTimeout(total=8)
    try:
        # 1. Get DIRECT outbound IP
        try:
            async with session.get(
                "https://api.ipify.org?format=json", timeout=timeout,
            ) as resp:
                result["direct_ip"] = await resp.json()
        except Exception as exc:
            result["direct_ip_error"] = str(exc)

        # 2. Check Polymarket geoblock (DIRECT)
        try:
            async with session.get(
                "https://polymarket.com/api/geoblock", timeout=timeout,
            ) as resp:
                result["direct_geoblock"] = await resp.json()
        except Exception as exc:
            result["direct_geoblock_error"] = str(exc)

        # 3. If proxy configured, test via proxy
        result["proxy_configured"] = bool(proxy_url)
        if proxy_url:
            result["proxy_host"] = proxy_url.split("@")[-1] if "@" in proxy_url else proxy_url.replace("http://", "").replace("https://", "")
            try:
                async with session.get(
                    "https://api.ipify.org?format=json", proxy=proxy_url, timeout=timeout,
                ) as resp:
                    result["proxy_ip"] = await resp.json()
            except Exception as exc:
                result["proxy_ip_error"] = str(exc)

            try:
                async with session.get(
                    "https://polymarket.com/api/geoblock", proxy=proxy_url, timeout=timeout,
                ) as resp:
                    result["proxy_geoblock"] = await resp.json()
            except Exception as exc:
                result["proxy_geoblock_error"] = str(exc)

        return web.json_response(result)
    finally:
        if close_session:
            await session.close()


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    """Browser WebSocket connection — /ws"""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    CLIENTS.add(ws)
    logger.info("Browser client connected  (total: %d)", len(CLIENTS))

    try:
        # Drain incoming frames; browsers only send close frames here
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(ws)
        logger.info("Browser client disconnected (total: %d)", len(CLIENTS))

    return ws


async def index_handler(request: web.Request) -> web.Response:
    """Serve the single-page frontend — /"""
    return web.Response(text=HTML, content_type="text/html")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

_runner: web.AppRunner | None = None


async def start_server(host: str = "localhost", port: int = 8765) -> None:
    """
    Start the HTTP + WebSocket server inside the running event loop.
    Uses AppRunner + TCPSite so it does not block — control returns immediately
    and aiohttp serves connections during asyncio yield points.
    """
    global _runner

    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/api/potential-trades", api_potential_trades)
    app.router.add_get("/api/actual-trades", api_actual_trades)
    app.router.add_get("/api/wallet-balance", api_wallet_balance)
    app.router.add_get("/api/debug-ip", api_debug_ip)

    # handle_signals=False prevents aiohttp from installing its own SIGTERM
    # handler, which would conflict with asyncio.run()'s signal handling.
    _runner = web.AppRunner(app, handle_signals=False)
    await _runner.setup()

    site = web.TCPSite(_runner, host, port)
    await site.start()
    logger.info("Web UI → http://%s:%d", host, port)


async def stop_server() -> None:
    global _runner
    if _runner is not None:
        await _runner.cleanup()
        _runner = None
        logger.info("Web server stopped.")


# ---------------------------------------------------------------------------
# Frontend HTML (served inline — no static file needed)
# ---------------------------------------------------------------------------

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket BTC 15m — Live Feed</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0d1117;
      color: #e6edf3;
      font-family: 'Menlo', 'Consolas', 'Monaco', monospace;
      font-size: 12px;
      line-height: 1.5;
      display: flex;
      flex-direction: column;
      height: 100vh;
      overflow: hidden;
    }

    /* ── Header ─────────────────────────────────────────────────────────── */
    #header {
      flex-shrink: 0;
      background: #161b22;
      border-bottom: 1px solid #30363d;
      padding: 10px 16px;
      display: flex;
      align-items: center;
      gap: 14px;
    }

    #header h1 { font-size: 13px; font-weight: 600; color: #e6edf3; }

    #status {
      padding: 2px 9px;
      border-radius: 12px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.04em;
    }
    .connected    { background: #1a4731; color: #3fb950; }
    .disconnected { background: #3d1e1e; color: #f85149; }

    #proxy-badge {
      padding: 2px 9px;
      border-radius: 12px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.04em;
      cursor: pointer;
    }
    .proxy-none    { background: #3d2e1e; color: #d29922; }
    .proxy-ok      { background: #1a4731; color: #3fb950; }
    .proxy-blocked { background: #3d1e1e; color: #f85149; }
    .proxy-testing { background: #1e2d3d; color: #58a6ff; }

    #count { color: #8b949e; font-size: 11px; margin-left: auto; }

    /* ── Top-level tab bar ──────────────────────────────────────────────── */
    #top-tab-bar {
      flex-shrink: 0;
      display: flex;
      background: #161b22;
      border-bottom: 1px solid #30363d;
      padding: 0 12px;
    }

    .top-tab {
      padding: 10px 24px;
      background: none;
      border: none;
      border-bottom: 3px solid transparent;
      color: #8b949e;
      cursor: pointer;
      font-family: inherit;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
      transition: color 0.1s;
    }
    .top-tab:hover  { color: #e6edf3; }
    .top-tab.active { color: #e6edf3; border-bottom-color: #58a6ff; }

    /* ── Subtab bars ────────────────────────────────────────────────────── */
    .sub-tab-bar {
      flex-shrink: 0;
      display: none;
      background: #0d1117;
      border-bottom: 1px solid #21262d;
      padding: 0 12px;
      overflow-x: auto;
      scrollbar-width: none;
    }
    .sub-tab-bar::-webkit-scrollbar { display: none; }
    .sub-tab-bar.active { display: flex; }

    .tab {
      padding: 8px 16px;
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      color: #8b949e;
      cursor: pointer;
      font-family: inherit;
      font-size: 12px;
      white-space: nowrap;
      transition: color 0.1s;
    }
    .tab:hover  { color: #e6edf3; }
    .tab.active { color: #e6edf3; border-bottom-color: #e6edf3; }

    .tab-bba.active   { color: #388bfd; border-bottom-color: #388bfd; }
    .tab-trade.active { color: #3fb950; border-bottom-color: #3fb950; }
    .tab-pc.active    { color: #d29922; border-bottom-color: #d29922; }
    .tab-book.active  { color: #6e7681; border-bottom-color: #6e7681; }
    .tab-tick.active  { color: #f778ba; border-bottom-color: #f778ba; }

    .badge {
      display: inline-block;
      background: #21262d;
      color: #8b949e;
      border-radius: 10px;
      padding: 0 6px;
      font-size: 10px;
      margin-left: 5px;
      min-width: 20px;
      text-align: center;
    }
    .tab.active .badge { background: #30363d; color: #e6edf3; }

    /* ── Main area (list + detail panel side by side) ────────────────────── */
    #main {
      flex: 1;
      display: flex;
      overflow: hidden;
    }

    /* ── Tab panes ───────────────────────────────────────────────────────── */
    #tab-content {
      flex: 1;
      overflow: hidden;
      position: relative;
      min-width: 0;
    }

    .tab-pane {
      display: none;
      position: absolute;
      inset: 0;
      overflow-y: auto;
      padding: 6px 12px;
    }
    .tab-pane.active { display: block; }

    /* ── Entries ─────────────────────────────────────────────────────────── */
    .entry {
      border-left: 3px solid #444;
      margin: 3px 0;
      padding: 5px 10px;
      background: #0d1117;
      border-radius: 0 4px 4px 0;
    }
    .entry:hover    { background: #161b22; }
    .entry.selected { background: #161b22; }

    .entry-header {
      display: flex;
      align-items: baseline;
      gap: 0;
      flex-wrap: wrap;
      user-select: none;
    }

    .ts { color: #6e7681; font-size: 11px; padding-right: 8px; flex-shrink: 0; }

    /* ── Event chips (one per event on the row) ──────────────────────────── */
    .event-chip {
      display: inline-flex;
      align-items: baseline;
      gap: 5px;
      padding: 1px 8px;
      border-left: 1px solid #21262d;
      cursor: pointer;
      border-radius: 2px;
    }
    .event-chip:first-of-type { border-left: none; padding-left: 0; }
    .event-chip:hover    { background: #1c2433; }
    .event-chip.selected { background: #1c2433; outline: 1px solid #388bfd55; border-radius: 2px; }

    .label   { font-weight: 700; font-size: 11px; }
    .Up      { color: #3fb950; }
    .Down    { color: #f85149; }
    .etype   { font-weight: 600; font-size: 11px; color: #e6edf3; }
    .summary { color: #8b949e; font-size: 11px; }

    .ask-sum  { font-size: 11px; font-weight: 600; min-width: 54px; text-align: right; padding-right: 10px; flex-shrink: 0; letter-spacing: 0.02em; }
    .sum-arb  { color: #d29922; }
    .sum-ok   { color: #444c56; }
    .sum-null { color: #2d333b; }

    .best_bid_ask     { border-color: #388bfd; }
    .last_trade_price { border-color: #3fb950; }
    .price_change     { border-color: #d29922; }
    .book             { border-color: #6e7681; }
    .tick_size        { border-color: #f778ba; }

    /* ── Detail panel ────────────────────────────────────────────────────── */
    #detail-panel {
      width: 0;
      flex-shrink: 0;
      display: flex;
      flex-direction: column;
      background: #010409;
      border-left: 1px solid #30363d;
      overflow: hidden;
      transition: width 0.15s ease;
    }
    #detail-panel.open {
      width: 40%;
    }

    #detail-header {
      flex-shrink: 0;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 12px;
      background: #161b22;
      border-bottom: 1px solid #30363d;
    }

    #detail-title {
      font-size: 11px;
      font-weight: 600;
      color: #8b949e;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    #detail-close {
      background: none;
      border: none;
      color: #6e7681;
      cursor: pointer;
      font-size: 14px;
      line-height: 1;
      padding: 0 0 0 12px;
      flex-shrink: 0;
    }
    #detail-close:hover { color: #e6edf3; }

    #detail-body {
      flex: 1;
      overflow-y: auto;
      padding: 12px;
      font-size: 11px;
      color: #adbac7;
      line-height: 1.6;
    }
    #detail-body.raw-json {
      white-space: pre;
    }

    /* ── Arb callout ─────────────────────────────────────────────────────── */
    #callout {
      flex-shrink: 0;
      display: flex;
      flex-direction: column;
      background: #0d1117;
      border-top: 2px solid #3d2800;
      max-height: 130px;
      overflow: hidden;
      transition: max-height 0.15s ease;
    }
    #callout.collapsed { max-height: 30px; }
    #callout-header {
      flex-shrink: 0;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 5px 12px;
      background: #160e00;
      border-bottom: 1px solid #3d2800;
      cursor: pointer;
      user-select: none;
    }
    #callout-title  { color: #d29922; font-weight: 700; font-size: 11px; letter-spacing: 0.05em; }
    #callout-toggle { margin-left: auto; color: #8b949e; font-size: 11px; }
    #callout-body   { overflow-y: auto; flex: 1; padding: 3px 12px; }

    .callout-row {
      display: flex;
      align-items: baseline;
      gap: 14px;
      padding: 2px 0;
      border-bottom: 1px solid #1a1000;
      font-size: 11px;
      white-space: nowrap;
    }
    .callout-dot.open  { color: #3fb950; }
    .callout-dot.close { color: #f85149; }
    .callout-ts     { color: #6e7681; }
    .callout-label  { font-weight: 700; min-width: 70px; }
    .arb-open  .callout-label { color: #3fb950; }
    .arb-close .callout-label { color: #8b949e; }
    .callout-detail { color: #8b949e; }
    .callout-sum    { color: #d29922; font-weight: 600; }
    .callout-gap    { color: #3fb950; font-weight: 700; }

    /* Arb-hit highlight in the live feed */
    .entry.arb-hit       { background: #120d00; border-left-color: #d29922 !important; }
    .entry.arb-hit:hover { background: #1a1200; }

    /* ── Dashboard tabs ─────────────────────────────────────────────────── */
    .tab-potential.active { color: #a371f7; border-bottom-color: #a371f7; }
    .tab-actual.active    { color: #f0883e; border-bottom-color: #f0883e; }

    .dash-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }
    .dash-table th {
      position: sticky;
      top: 0;
      background: #161b22;
      color: #8b949e;
      font-weight: 600;
      text-align: left;
      padding: 6px 10px;
      border-bottom: 1px solid #30363d;
      white-space: nowrap;
    }
    .dash-table td {
      padding: 4px 10px;
      border-bottom: 1px solid #21262d;
      white-space: nowrap;
    }
    .dash-table tr:hover { background: #161b22; }

    .status-icon { font-size: 13px; text-align: center; }
    .status-both   { color: #3fb950; }
    .status-fail   { color: #f85149; }
    .status-dry    { color: #8b949e; }
    .status-oneleg { color: #d29922; }

    .stats-bar {
      display: flex;
      gap: 20px;
      padding: 8px 12px;
      background: #161b22;
      border-bottom: 1px solid #30363d;
      font-size: 11px;
      color: #8b949e;
      flex-wrap: wrap;
    }
    .stat-value    { color: #e6edf3; font-weight: 600; }
    .stat-positive { color: #3fb950; }
    .stat-negative { color: #f85149; }

    .dash-empty {
      padding: 40px;
      text-align: center;
      color: #484f58;
      font-size: 12px;
    }
    .dash-link { color: #388bfd; text-decoration: none; }
    .dash-link:hover { text-decoration: underline; }

    .dash-table tr.clickable { cursor: pointer; }
    .dash-table tr.clickable:hover { background: #1c2433; }
    .dash-table tr.clickable.selected-row { background: #1c2433; outline: 1px solid #388bfd44; }

    /* ── Trade debug detail sections ───────────────────────────────────── */
    .debug-section {
      margin-bottom: 14px;
    }
    .debug-section-title {
      color: #58a6ff;
      font-weight: 700;
      font-size: 11px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      margin-bottom: 6px;
      border-bottom: 1px solid #21262d;
      padding-bottom: 3px;
    }
    .debug-grid {
      display: grid;
      grid-template-columns: 140px 1fr;
      gap: 2px 12px;
      font-size: 11px;
    }
    .debug-label {
      color: #6e7681;
      white-space: nowrap;
    }
    .debug-value {
      color: #e6edf3;
      word-break: break-all;
    }
    .debug-value.success { color: #3fb950; }
    .debug-value.fail    { color: #f85149; }
    .debug-value.warn    { color: #d29922; }
    .debug-value.muted   { color: #484f58; }
    .debug-json {
      background: #0d1117;
      border: 1px solid #21262d;
      border-radius: 4px;
      padding: 8px;
      margin-top: 4px;
      font-size: 10px;
      max-height: 200px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
      color: #adbac7;
    }

    /* ── Wallet balance card ──────────────────────────────────────────── */
    .wallet-card {
      display: flex;
      align-items: center;
      gap: 24px;
      padding: 10px 16px;
      margin: 6px 0;
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 6px;
      font-size: 11px;
    }
    .wallet-card .balance-item {
      display: flex;
      align-items: baseline;
      gap: 6px;
    }
    .wallet-card .balance-label {
      color: #8b949e;
      font-weight: 600;
    }
    .wallet-card .balance-value {
      color: #e6edf3;
      font-weight: 700;
      font-size: 13px;
    }
    .wallet-card .balance-value.usdc { color: #3fb950; }
    .wallet-card .balance-value.pol  { color: #a371f7; }
    .wallet-card .wallet-addr {
      color: #6e7681;
      font-size: 10px;
      margin-left: auto;
    }
  </style>
</head>
<body>

<div id="header">
  <h1>Polymarket BTC 15m — Live Feed</h1>
  <span id="status" class="disconnected">DISCONNECTED</span>
  <span id="proxy-badge" class="proxy-none" title="Click to test proxy">NO PROXY</span>
  <span id="count">0 events</span>
</div>

<!-- Top-level tabs -->
<div id="top-tab-bar">
  <button class="top-tab active" data-top="trading">Trading</button>
  <button class="top-tab" data-top="ws">WS</button>
</div>

<!-- Subtab bars (only one visible at a time) -->
<div id="sub-tab-bar-trading" class="sub-tab-bar active">
  <button class="tab tab-potential active" data-tab="potential">Potential Trades <span class="badge" id="badge-potential">0</span></button>
  <button class="tab tab-actual"           data-tab="actual">   Actual Trades   <span class="badge" id="badge-actual">&mdash;</span></button>
</div>

<div id="sub-tab-bar-ws" class="sub-tab-bar">
  <button class="tab active"    data-tab="all">             All              <span class="badge" id="badge-all">0</span></button>
  <button class="tab tab-bba"   data-tab="best_bid_ask">    best_bid_ask     <span class="badge" id="badge-best_bid_ask">0</span></button>
  <button class="tab tab-trade" data-tab="last_trade_price">last_trade_price <span class="badge" id="badge-last_trade_price">0</span></button>
  <button class="tab tab-pc"    data-tab="price_change">    price_change     <span class="badge" id="badge-price_change">0</span></button>
  <button class="tab tab-book"  data-tab="book">            book             <span class="badge" id="badge-book">0</span></button>
  <button class="tab tab-tick"  data-tab="tick_size">       tick_size        <span class="badge" id="badge-tick_size">0</span></button>
</div>

<div id="main">
  <div id="tab-content">
    <div class="tab-pane active" id="pane-potential"></div>
    <div class="tab-pane"        id="pane-actual"></div>
    <div class="tab-pane"        id="pane-all"></div>
    <div class="tab-pane"        id="pane-best_bid_ask"></div>
    <div class="tab-pane"        id="pane-last_trade_price"></div>
    <div class="tab-pane"        id="pane-price_change"></div>
    <div class="tab-pane"        id="pane-book"></div>
    <div class="tab-pane"        id="pane-tick_size"></div>
  </div>

  <div id="detail-panel">
    <div id="detail-header">
      <span id="detail-title">&mdash;</span>
      <button id="detail-close" title="Close">&#x2715;</button>
    </div>
    <div id="detail-body"></div>
  </div>
</div>

<div id="callout">
  <div id="callout-header">
    <span>&#x26A1;</span>
    <span id="callout-title">ARB SCANNER &mdash; Up ask + Down ask &lt; 1</span>
    <span id="callout-count" class="badge">0</span>
    <span id="callout-toggle">&#x25B2;</span>
  </div>
  <div id="callout-body"></div>
</div>

<script>
  const statusEl     = document.getElementById('status');
  const countEl      = document.getElementById('count');
  const detailPanel  = document.getElementById('detail-panel');
  const detailTitle  = document.getElementById('detail-title');
  const detailBody   = document.getElementById('detail-body');
  const detailClose  = document.getElementById('detail-close');

  // All WS event types that get their own pane (plus 'all' catch-all)
  const WS_TABS = ['all', 'best_bid_ask', 'last_trade_price', 'price_change', 'book', 'tick_size'];
  const MAX_ENTRIES = 200;

  // Badge counts for all tabs
  const counts = {};
  WS_TABS.forEach(t => counts[t] = 0);
  counts.potential = 0;

  let selectedChip = null;

  // ── Two-level tab state ─────────────────────────────────────────────────
  let activeTopTab = 'trading';
  const activeSubTab = { ws: 'all', trading: 'potential' };

  // Top-tab click handler
  document.querySelectorAll('.top-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const topKey = btn.dataset.top;
      if (topKey === activeTopTab) return;

      document.querySelectorAll('.top-tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.sub-tab-bar').forEach(b => b.classList.remove('active'));

      btn.classList.add('active');
      document.getElementById('sub-tab-bar-' + topKey).classList.add('active');
      activeTopTab = topKey;

      switchSubTab(activeSubTab[topKey]);
    });
  });

  function switchSubTab(tabKey) {
    // Hide all panes
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));

    // Deactivate all subtabs in the current top tab's bar
    const barId = 'sub-tab-bar-' + activeTopTab;
    document.querySelectorAll('#' + barId + ' .tab').forEach(t => t.classList.remove('active'));

    // Activate the target subtab button
    const targetBtn = document.querySelector('#' + barId + ' .tab[data-tab="' + tabKey + '"]');
    if (targetBtn) targetBtn.classList.add('active');

    // Show the pane
    const pane = document.getElementById('pane-' + tabKey);
    if (pane) pane.classList.add('active');

    // Remember this subtab
    activeSubTab[activeTopTab] = tabKey;

    // Manage polling
    managePoll(tabKey);
  }

  // Subtab click handlers
  document.querySelectorAll('.sub-tab-bar .tab').forEach(btn => {
    btn.addEventListener('click', () => {
      switchSubTab(btn.dataset.tab);
    });
  });

  function managePoll(tabKey) {
    stopPotentialPoll();
    stopActualPoll();
    stopBalancePoll();

    if (tabKey === 'potential') {
      startPotentialPoll();
    } else if (tabKey === 'actual') {
      startActualPoll();
      startBalancePoll();
    }
  }

  // Pause polling when browser tab is hidden
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      stopPotentialPoll();
      stopActualPoll();
      stopBalancePoll();
    } else {
      managePoll(activeSubTab[activeTopTab]);
    }
  });

  // ── Arb scanner state ─────────────────────────────────────────────────────
  const calloutBody  = document.getElementById('callout-body');
  const calloutCount = document.getElementById('callout-count');
  const calloutEl    = document.getElementById('callout');
  const latestAsk    = { Up: null, Down: null };
  const MAX_CALLOUT  = 50;
  let arbActive = false;
  let arbCount  = 0;

  document.getElementById('callout-header').addEventListener('click', () => {
    calloutEl.classList.toggle('collapsed');
    document.getElementById('callout-toggle').textContent =
      calloutEl.classList.contains('collapsed') ? '\\u25BC' : '\\u25B2';
  });

  // ── Detail panel ──────────────────────────────────────────────────────────

  function openDetail(chip, data, ts) {
    if (selectedChip && selectedChip !== chip) {
      selectedChip.classList.remove('selected');
      selectedChip.closest('.entry').classList.remove('selected');
    }
    if (selectedChip === chip && detailPanel.classList.contains('open')) {
      closeDetail();
      return;
    }
    selectedChip = chip;
    chip.classList.add('selected');
    chip.closest('.entry').classList.add('selected');
    detailTitle.textContent = '[' + data.label + ']  ' + data.event_type + '  ' + ts;
    detailBody.className = 'raw-json';
    detailBody.textContent  = JSON.stringify(data, null, 2);
    detailPanel.classList.add('open');
  }

  function closeDetail() {
    detailPanel.classList.remove('open');
    if (selectedChip) {
      selectedChip.classList.remove('selected');
      selectedChip.closest('.entry').classList.remove('selected');
      selectedChip = null;
    }
    // Also clear trade detail selection
    if (selectedTradeIdx !== null) {
      document.querySelectorAll('#potential-table tr.selected-row').forEach(function(r) {
        r.classList.remove('selected-row');
      });
      selectedTradeIdx = null;
    }
  }

  detailClose.addEventListener('click', closeDetail);

  // ── Helpers ───────────────────────────────────────────────────────────────

  function fmt(n, digits) {
    if (digits === undefined) digits = 4;
    return n == null ? '\\u2014' : Number(n).toFixed(digits);
  }

  function makeSummary(d) {
    switch (d.event_type) {
      case 'best_bid_ask':
        return 'bid ' + fmt(d.best_bid) + '  ask ' + fmt(d.best_ask) + '  mid ' + fmt(d.mid);
      case 'last_trade_price':
        return d.last_trade_side + ' ' + fmt(d.last_trade) + ' \\u00D7 ' + fmt(d.last_trade_size, 1);
      case 'price_change':
        return 'bid ' + fmt(d.best_bid) + '  ask ' + fmt(d.best_ask);
      case 'book':
        return 'snapshot  bid ' + fmt(d.best_bid) + '  ask ' + fmt(d.best_ask);
      case 'tick_size':
        return 'tick_size: ' + (d.raw && d.raw.tick_size ? d.raw.tick_size : '\\u2014');
      default:
        return '';
    }
  }

  // ── Chip + grouped-row rendering ─────────────────────────────────────────

  const lastPaneEntry = {};

  function makeChip(data, ts) {
    const chip = document.createElement('span');
    chip.className = 'event-chip ' + data.event_type;
    chip.innerHTML =
      '<span class="label ' + data.label + '">[' + data.label + ']</span>' +
      '<span class="etype">' + data.event_type + '</span>' +
      '<span class="summary">' + makeSummary(data) + '</span>';
    chip.addEventListener('click', function(e) {
      e.stopPropagation();
      openDetail(chip, data, ts);
    });
    return chip;
  }

  function makeEntry(data, ts, showSum) {
    const entry = document.createElement('div');
    entry.className = 'entry ' + data.event_type;
    const header = document.createElement('div');
    header.className = 'entry-header';

    if (showSum) {
      const sumSpan = document.createElement('span');
      const up = latestAsk.Up;
      const dn = latestAsk.Down;
      if (up != null && dn != null) {
        const s = up + dn;
        sumSpan.textContent = s.toFixed(4);
        sumSpan.className = 'ask-sum ' + (s < 1.0 ? 'sum-arb' : 'sum-ok');
      } else {
        sumSpan.textContent = '\\u2014\\u2014';
        sumSpan.className = 'ask-sum sum-null';
      }
      header.appendChild(sumSpan);
    }

    const tsSpan = document.createElement('span');
    tsSpan.className = 'ts';
    tsSpan.textContent = ts;
    header.appendChild(tsSpan);
    header.appendChild(makeChip(data, ts));
    entry.appendChild(header);
    return entry;
  }

  function trimPane(pane) {
    while (pane.children.length > MAX_ENTRIES) {
      pane.removeChild(pane.firstChild);
    }
  }

  function addToPane(paneId, data, ts, showSum) {
    const pane = document.getElementById(paneId);
    if (!pane) return;
    const last = lastPaneEntry[paneId];
    if (last && last.ts === ts) {
      last.el.querySelector('.entry-header').appendChild(makeChip(data, ts));
    } else {
      const entry = makeEntry(data, ts, showSum);
      pane.appendChild(entry);
      lastPaneEntry[paneId] = { el: entry, ts: ts };
      trimPane(pane);
    }
  }

  // ── Render ────────────────────────────────────────────────────────────────

  function addEntry(data) {
    const ts  = new Date().toISOString().slice(11, 23);
    const key = data.event_type;

    if (key === 'best_bid_ask' && data.best_ask != null) {
      latestAsk[data.label] = data.best_ask;
    }

    counts.all++;
    addToPane('pane-all', data, ts, false);
    var badgeAll = document.getElementById('badge-all');
    if (badgeAll) badgeAll.textContent = counts.all;

    if (key in counts) {
      counts[key]++;
      addToPane('pane-' + key, data, ts, key === 'best_bid_ask');
      var badge = document.getElementById('badge-' + key);
      if (badge) badge.textContent = counts[key];
    }

    countEl.textContent = counts.all.toLocaleString() + ' events';
    checkArb(data, ts);
  }

  // ── Arb check ─────────────────────────────────────────────────────────────

  function checkArb(data, ts) {
    if (data.event_type !== 'best_bid_ask' || data.best_ask == null) return;

    const upAsk   = latestAsk.Up;
    const downAsk = latestAsk.Down;
    if (upAsk == null || downAsk == null) return;

    const sum   = upAsk + downAsk;
    const isArb = sum < 1.0;
    if (isArb === arbActive) return;
    arbActive = isArb;

    if (isArb) {
      for (var i = 0; i < 2; i++) {
        var paneId = i === 0 ? 'pane-all' : 'pane-best_bid_ask';
        var last = lastPaneEntry[paneId];
        if (last && last.ts === ts) last.el.classList.add('arb-hit');
      }
    }

    arbCount++;
    calloutCount.textContent = arbCount;

    var gap = (1.0 - sum) * 100;
    var row = document.createElement('div');
    row.className = 'callout-row ' + (isArb ? 'arb-open' : 'arb-close');
    row.innerHTML = isArb
      ? '<span class="callout-dot open">\\u25CF</span>' +
        '<span class="callout-ts">' + ts + '</span>' +
        '<span class="callout-label">OPENED</span>' +
        '<span class="callout-detail">Up ' + upAsk.toFixed(4) + ' + Down ' + downAsk.toFixed(4) + '</span>' +
        '<span class="callout-sum">= ' + sum.toFixed(4) + '</span>' +
        '<span class="callout-gap">+' + gap.toFixed(2) + '% edge</span>'
      : '<span class="callout-dot close">\\u25CF</span>' +
        '<span class="callout-ts">' + ts + '</span>' +
        '<span class="callout-label">CLOSED</span>' +
        '<span class="callout-detail">Up ' + upAsk.toFixed(4) + ' + Down ' + downAsk.toFixed(4) + '</span>' +
        '<span class="callout-sum">= ' + sum.toFixed(4) + '</span>';

    calloutBody.insertBefore(row, calloutBody.firstChild);
    while (calloutBody.children.length > MAX_CALLOUT) {
      calloutBody.removeChild(calloutBody.lastChild);
    }
  }

  // ── Potential Trades polling ──────────────────────────────────────────────
  var potentialTimer = null;
  var POTENTIAL_MS = 3000;

  function startPotentialPoll() {
    if (potentialTimer) return;
    fetchPotentialTrades();
    potentialTimer = setInterval(fetchPotentialTrades, POTENTIAL_MS);
  }
  function stopPotentialPoll() {
    if (potentialTimer) { clearInterval(potentialTimer); potentialTimer = null; }
  }

  async function fetchPotentialTrades() {
    try {
      var resp = await fetch('/api/potential-trades');
      var data = await resp.json();
      renderPotentialTrades(data.trades || [], data.stats || {});
    } catch (e) { console.error('Potential trades fetch error:', e); }
  }

  function statusIcon(status) {
    switch (status) {
      case 'both_filled':    return { ch: '\\u2713', cls: 'status-both' };
      case 'one_leg':        return { ch: '\\u26A0', cls: 'status-oneleg' };
      case 'neither_filled': return { ch: '\\u2717', cls: 'status-fail' };
      case 'dry_run':        return { ch: '\\u2014', cls: 'status-dry' };
      default:               return { ch: '\\u2717', cls: 'status-fail' };
    }
  }

  var potentialTradesData = [];
  var selectedTradeIdx = null;

  function renderPotentialTrades(trades, stats) {
    potentialTradesData = trades;
    var pane = document.getElementById('pane-potential');
    document.getElementById('badge-potential').textContent = trades.length || '0';

    var html = '<div class="stats-bar">';
    html += '<span>Detected: <span class="stat-value">' + (stats.arbs_detected || 0) + '</span></span>';
    html += '<span>Attempted: <span class="stat-value">' + (stats.arbs_attempted || 0) + '</span></span>';
    html += '<span>Both filled: <span class="stat-value stat-positive">' + (stats.arbs_both_filled || 0) + '</span></span>';
    html += '<span>One leg: <span class="stat-value stat-negative">' + (stats.arbs_one_leg || 0) + '</span></span>';
    html += '<span>Neither: <span class="stat-value">' + (stats.arbs_neither_filled || 0) + '</span></span>';
    html += '<span>Edge captured: <span class="stat-value stat-positive">$' + (stats.total_edge_captured || 0).toFixed(4) + '</span></span>';
    html += '<span>Errors: <span class="stat-value ' + ((stats.errors || 0) > 0 ? 'stat-negative' : '') + '">' + (stats.errors || 0) + '</span></span>';
    html += '</div>';

    if (trades.length === 0) {
      html += '<div class="dash-empty">No arb opportunities detected yet. Waiting for spreads to compress below 1.00...</div>';
    } else {
      html += '<table class="dash-table" id="potential-table"><thead><tr>';
      html += '<th></th><th>Time</th><th>Up Ask</th><th>Down Ask</th><th>Total</th><th>Edge (bps)</th><th>Size</th><th>Status</th>';
      html += '</tr></thead><tbody>';
      for (var i = 0; i < trades.length; i++) {
        var t = trades[i];
        var icon = statusIcon(t.status);
        var ts = new Date(t.timestamp * 1000).toISOString().slice(11, 23);
        var selCls = (selectedTradeIdx === i) ? ' selected-row' : '';
        html += '<tr class="clickable' + selCls + '" data-idx="' + i + '">';
        html += '<td class="status-icon ' + icon.cls + '">' + icon.ch + '</td>';
        html += '<td>' + ts + '</td>';
        html += '<td>' + t.up_ask.toFixed(4) + '</td>';
        html += '<td>' + t.down_ask.toFixed(4) + '</td>';
        html += '<td>' + t.total.toFixed(4) + '</td>';
        html += '<td>' + t.edge_bps.toFixed(1) + '</td>';
        html += '<td>' + t.size.toFixed(1) + '</td>';
        html += '<td>' + t.status.replace(/_/g, ' ') + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
    }
    pane.innerHTML = html;

    // Attach click handlers to rows
    var tbl = document.getElementById('potential-table');
    if (tbl) {
      tbl.querySelectorAll('tr.clickable').forEach(function(row) {
        row.addEventListener('click', function() {
          var idx = parseInt(row.dataset.idx, 10);
          openTradeDetail(idx);
        });
      });
    }
  }

  function openTradeDetail(idx) {
    var t = potentialTradesData[idx];
    if (!t) return;

    // Toggle off if clicking same row
    if (selectedTradeIdx === idx && detailPanel.classList.contains('open')) {
      closeTradeDetail();
      return;
    }

    // Clear WS chip selection if any
    if (selectedChip) {
      selectedChip.classList.remove('selected');
      selectedChip.closest('.entry').classList.remove('selected');
      selectedChip = null;
    }

    // Highlight selected row
    document.querySelectorAll('#potential-table tr.selected-row').forEach(function(r) {
      r.classList.remove('selected-row');
    });
    var row = document.querySelector('#potential-table tr[data-idx="' + idx + '"]');
    if (row) row.classList.add('selected-row');
    selectedTradeIdx = idx;

    var ts = new Date(t.timestamp * 1000).toISOString().slice(0, 23).replace('T', ' ');
    detailTitle.textContent = 'Trade #' + (idx + 1) + '  \\u2014  ' + ts + ' UTC';

    var html = '';

    // ── Overview section
    html += '<div class="debug-section">';
    html += '<div class="debug-section-title">Overview</div>';
    html += '<div class="debug-grid">';
    html += kv('Timestamp', ts + ' UTC');
    html += kv('Status', t.status.replace(/_/g, ' '), t.status === 'both_filled' ? 'success' : t.status === 'dry_run' ? 'muted' : 'fail');
    html += kv('Mode', t.mode || '\\u2014', t.mode === 'LIVE' ? 'success' : 'muted');
    html += kv('Generation', t.generation != null ? t.generation : '\\u2014');
    html += kv('Up Ask', t.up_ask.toFixed(4));
    html += kv('Down Ask', t.down_ask.toFixed(4));
    html += kv('Total', t.total.toFixed(4), t.total < 1.0 ? 'warn' : '');
    html += kv('Edge', t.edge_bps.toFixed(1) + ' bps');
    html += kv('Size', t.size.toFixed(1) + ' shares');
    html += '</div></div>';

    // ── Timing section
    html += '<div class="debug-section">';
    html += '<div class="debug-section-title">Timing</div>';
    html += '<div class="debug-grid">';
    html += kv('Total duration', fmtMs(t.total_duration_ms));
    html += kv('Sign duration', fmtMs(t.sign_duration_ms));
    if (t.up_result) html += kv('Up POST latency', fmtMs(t.up_result.post_latency_ms));
    if (t.down_result) html += kv('Down POST latency', fmtMs(t.down_result.post_latency_ms));
    html += '</div></div>';

    // ── Up Leg section
    html += legSection('Up Leg', t.up_result);

    // ── Down Leg section
    html += legSection('Down Leg', t.down_result);

    detailBody.className = '';
    detailBody.innerHTML = html;
    detailPanel.classList.add('open');
  }

  function closeTradeDetail() {
    detailPanel.classList.remove('open');
    document.querySelectorAll('#potential-table tr.selected-row').forEach(function(r) {
      r.classList.remove('selected-row');
    });
    selectedTradeIdx = null;
  }

  function kv(label, value, cls) {
    return '<span class="debug-label">' + label + '</span>' +
      '<span class="debug-value' + (cls ? ' ' + cls : '') + '">' + value + '</span>';
  }

  function fmtMs(v) {
    if (v == null) return '<span class="debug-value muted">\\u2014</span>';
    return v.toFixed(1) + ' ms';
  }

  function legSection(title, leg) {
    var html = '<div class="debug-section">';
    html += '<div class="debug-section-title">' + title + '</div>';
    if (!leg) {
      html += '<span class="debug-value muted">No data (dry run)</span>';
      html += '</div>';
      return html;
    }
    html += '<div class="debug-grid">';
    html += kv('Success', leg.success ? 'YES' : 'NO', leg.success ? 'success' : 'fail');
    html += kv('Price', Number(leg.price).toFixed(4));
    html += kv('Size', Number(leg.size).toFixed(1));
    html += kv('HTTP Status', leg.http_status != null ? leg.http_status : '\\u2014',
      leg.http_status === 200 ? 'success' : leg.http_status ? 'fail' : 'muted');
    html += kv('Order ID', leg.order_id || '\\u2014');
    html += kv('CLOB Status', leg.status || '\\u2014');
    html += kv('Error', leg.error || '\\u2014', leg.error ? 'fail' : 'muted');
    html += kv('POST Latency', fmtMs(leg.post_latency_ms));
    html += kv('Token ID', leg.token_id ? leg.token_id.slice(0, 20) + '...' : '\\u2014');
    html += '</div>';

    if (leg.response_body) {
      html += '<div style="margin-top:6px"><span class="debug-label">API Response:</span>';
      html += '<div class="debug-json">' + escHtml(JSON.stringify(leg.response_body, null, 2)) + '</div></div>';
    }
    if (leg.request_body) {
      var reqObj;
      try { reqObj = JSON.parse(leg.request_body); } catch(e) { reqObj = leg.request_body; }
      html += '<div style="margin-top:6px"><span class="debug-label">Request Body:</span>';
      html += '<div class="debug-json">' + escHtml(JSON.stringify(reqObj, null, 2)) + '</div></div>';
    }
    html += '</div>';
    return html;
  }

  function escHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // ── Actual Trades polling ───────────────────────────────────────────────
  var actualTimer = null;
  var ACTUAL_MS = 15000;

  function startActualPoll() {
    if (actualTimer) return;
    fetchActualTrades();
    actualTimer = setInterval(fetchActualTrades, ACTUAL_MS);
  }
  function stopActualPoll() {
    if (actualTimer) { clearInterval(actualTimer); actualTimer = null; }
  }

  async function fetchActualTrades() {
    try {
      var resp = await fetch('/api/actual-trades');
      var data = await resp.json();
      if (data.error) { console.warn('Actual trades:', data.error); return; }
      renderActualTrades(Array.isArray(data) ? data : []);
    } catch (e) { console.error('Actual trades fetch error:', e); }
  }

  // ── Wallet Balance polling ──────────────────────────────────────────────
  var balanceTimer = null;
  var BALANCE_MS = 30000;
  var cachedBalance = null;

  function startBalancePoll() {
    if (balanceTimer) return;
    fetchBalance();
    balanceTimer = setInterval(fetchBalance, BALANCE_MS);
  }
  function stopBalancePoll() {
    if (balanceTimer) { clearInterval(balanceTimer); balanceTimer = null; }
  }

  async function fetchBalance() {
    try {
      var resp = await fetch('/api/wallet-balance');
      var data = await resp.json();
      if (!data.error) {
        cachedBalance = data;
        if (activeSubTab[activeTopTab] === 'actual') {
          var card = document.querySelector('.wallet-card');
          if (card) {
            updateWalletCard();
          } else {
            // Card doesn't exist yet — re-render the whole pane so it appears
            fetchActualTrades();
          }
        }
      }
    } catch (e) { console.error('Balance fetch error:', e); }
  }

  function updateWalletCard() {
    var card = document.querySelector('.wallet-card');
    if (!card || !cachedBalance) return;
    var addrShort = cachedBalance.wallet
      ? cachedBalance.wallet.slice(0, 6) + '...' + cachedBalance.wallet.slice(-4)
      : '\\u2014';
    card.innerHTML =
      '<div class="balance-item"><span class="balance-label">USDC.e</span>' +
      '<span class="balance-value usdc">' +
      (cachedBalance.usdc_e != null ? '$' + cachedBalance.usdc_e.toFixed(2) : '\\u2014') +
      '</span></div>' +
      '<div class="balance-item"><span class="balance-label">POL</span>' +
      '<span class="balance-value pol">' +
      (cachedBalance.pol != null ? cachedBalance.pol.toFixed(4) : '\\u2014') +
      '</span></div>' +
      '<span class="wallet-addr">' + addrShort + '</span>';
  }

  function renderActualTrades(activities) {
    var pane = document.getElementById('pane-actual');
    document.getElementById('badge-actual').textContent = activities.length || '0';

    var html = '';

    // Wallet balance card
    if (cachedBalance) {
      var addrShort = cachedBalance.wallet
        ? cachedBalance.wallet.slice(0, 6) + '...' + cachedBalance.wallet.slice(-4)
        : '\\u2014';
      html += '<div class="wallet-card">';
      html += '<div class="balance-item"><span class="balance-label">USDC.e</span>';
      html += '<span class="balance-value usdc">' +
        (cachedBalance.usdc_e != null ? '$' + cachedBalance.usdc_e.toFixed(2) : '\\u2014') +
        '</span></div>';
      html += '<div class="balance-item"><span class="balance-label">POL</span>';
      html += '<span class="balance-value pol">' +
        (cachedBalance.pol != null ? cachedBalance.pol.toFixed(4) : '\\u2014') +
        '</span></div>';
      html += '<span class="wallet-addr">' + addrShort + '</span>';
      html += '</div>';
    }

    // Aggregate stats
    var totalCost = 0, totalSize = 0;
    var processed = activities.map(function(a) {
      var price = parseFloat(a.price) || 0;
      var size = parseFloat(a.size) || 0;
      var cost = price * size;
      totalCost += cost;
      totalSize += size;
      return Object.assign({}, a, { numPrice: price, numSize: size, cost: cost });
    });

    html += '<div class="stats-bar">';
    html += '<span>Total trades: <span class="stat-value">' + activities.length + '</span></span>';
    html += '<span>Total cost: <span class="stat-value">$' + totalCost.toFixed(4) + '</span></span>';
    html += '<span>Total size: <span class="stat-value">' + totalSize.toFixed(1) + ' shares</span></span>';
    html += '</div>';

    if (activities.length === 0) {
      html += '<div class="dash-empty">No trades executed yet. The bot will trade when arb opportunities appear with POLY_LIVE=1.</div>';
    } else {
      html += '<table class="dash-table"><thead><tr>';
      html += '<th>Time</th><th>Side</th><th>Outcome</th><th>Price</th><th>Size</th><th>Cost</th><th>Tx</th>';
      html += '</tr></thead><tbody>';
      for (var i = 0; i < processed.length; i++) {
        var t = processed[i];
        var ts = t.timestamp
          ? new Date(typeof t.timestamp === 'number' && t.timestamp < 1e12 ? t.timestamp * 1000 : t.timestamp).toISOString().slice(0, 19).replace('T', ' ')
          : '\\u2014';
        var txHash = t.transactionHash || '';
        var txLink = txHash
          ? '<a class="dash-link" href="https://polygonscan.com/tx/' + txHash + '" target="_blank">' + txHash.slice(0, 10) + '...</a>'
          : '\\u2014';
        html += '<tr>';
        html += '<td>' + ts + '</td>';
        html += '<td>' + (t.side || '\\u2014') + '</td>';
        html += '<td>' + (t.outcome || 'pending') + '</td>';
        html += '<td>' + t.numPrice.toFixed(4) + '</td>';
        html += '<td>' + t.numSize.toFixed(1) + '</td>';
        html += '<td>$' + t.cost.toFixed(4) + '</td>';
        html += '<td>' + txLink + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
    }
    pane.innerHTML = html;
  }

  // ── WebSocket ─────────────────────────────────────────────────────────────

  function connect() {
    var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    var ws = new WebSocket(proto + '//' + location.host + '/ws');

    ws.onopen = function() {
      statusEl.textContent = 'LIVE';
      statusEl.className   = 'connected';
    };

    ws.onclose = function() {
      statusEl.textContent = 'DISCONNECTED';
      statusEl.className   = 'disconnected';
      setTimeout(connect, 2000);
    };

    ws.onerror = function() { ws.close(); };

    ws.onmessage = function(evt) {
      try {
        addEntry(JSON.parse(evt.data));
      } catch (e) {
        console.error('Parse error:', e);
      }
    };
  }

  connect();

  // Start polling for the default active tab (Trading → Potential)
  managePoll(activeSubTab[activeTopTab]);

  // Proxy status check
  const proxyBadge = document.getElementById('proxy-badge');
  async function checkProxy() {
    proxyBadge.textContent = 'TESTING…';
    proxyBadge.className = 'proxy-testing';
    try {
      const r = await fetch('/api/debug-ip');
      const d = await r.json();
      if (!d.proxy_configured) {
        proxyBadge.textContent = 'NO PROXY';
        proxyBadge.className = 'proxy-none';
        proxyBadge.title = 'Direct IP: ' + (d.direct_ip?.ip || '?') +
          ' | Geoblock: ' + (d.direct_geoblock?.blocked ? 'BLOCKED' : 'OK');
      } else if (d.proxy_geoblock && !d.proxy_geoblock.blocked) {
        proxyBadge.textContent = 'PROXY OK';
        proxyBadge.className = 'proxy-ok';
        proxyBadge.title = 'Proxy IP: ' + (d.proxy_ip?.ip || '?') +
          ' | Direct: ' + (d.direct_ip?.ip || '?') + ' (blocked)';
      } else if (d.proxy_ip_error || d.proxy_geoblock_error) {
        proxyBadge.textContent = 'PROXY ERR';
        proxyBadge.className = 'proxy-blocked';
        proxyBadge.title = 'Proxy error: ' + (d.proxy_ip_error || d.proxy_geoblock_error);
      } else {
        proxyBadge.textContent = 'PROXY BLOCKED';
        proxyBadge.className = 'proxy-blocked';
        proxyBadge.title = 'Proxy IP: ' + (d.proxy_ip?.ip || '?') + ' — still geoblocked';
      }
    } catch(e) {
      proxyBadge.textContent = 'PROXY ?';
      proxyBadge.className = 'proxy-none';
      proxyBadge.title = 'Error checking: ' + e.message;
    }
  }
  proxyBadge.addEventListener('click', checkProxy);
  checkProxy();
</script>
</body>
</html>
"""
