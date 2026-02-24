"""
executor.py — Polymarket arbitrage execution engine.

ArbExecutor:
  - Registers as a PriceCallback on the BtcMarketIngestor
  - On each best_bid_ask event, checks the arb condition via fee lookup table
  - When profitable: builds, signs, and concurrently POSTs both legs
  - Handles partial fills, market rotation, cooldowns, and position tracking

Hot path (callback): ~200ns added — dict lookup only, returns immediately.
Cold path (execution): asyncio.create_task for signing + HTTP POSTs.
"""

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Optional

import aiohttp

from fees import is_arb, edge
from ws_ingestor import PriceEvent

if TYPE_CHECKING:
    from auth import PolyAuth
    from db import SupabaseWriter
    from ws_ingestor import BtcMarketIngestor

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"
POST_ORDER_PATH = "/order"
TICK_SIZE = "0.01"
NEG_RISK = False


class Mode(Enum):
    DRY_RUN = auto()  # Log arbs, do not send orders
    LIVE = auto()     # Actually submit orders


@dataclass
class LegResult:
    """Result of a single order leg submission."""
    side_label: str
    token_id: str
    price: float
    size: float
    success: bool
    order_id: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    # Debug fields
    http_status: Optional[int] = None
    response_body: Optional[dict] = None
    request_body: Optional[str] = None
    post_latency_ms: Optional[float] = None


@dataclass
class ArbAttempt:
    """Record of a completed arb attempt for P&L tracking."""
    timestamp: float
    up_ask: float
    down_ask: float
    edge_bps: float
    size: float
    up_result: Optional[LegResult] = None
    down_result: Optional[LegResult] = None
    # Debug fields
    generation: int = 0
    mode: str = "DRY_RUN"
    sign_duration_ms: Optional[float] = None
    total_duration_ms: Optional[float] = None

    @property
    def both_filled(self) -> bool:
        return (
            self.up_result is not None
            and self.up_result.success
            and self.down_result is not None
            and self.down_result.success
        )

    @property
    def one_leg_only(self) -> bool:
        if self.up_result is None or self.down_result is None:
            return False
        return self.up_result.success != self.down_result.success


@dataclass
class ExecutorStats:
    """Running statistics."""
    arbs_detected: int = 0
    arbs_attempted: int = 0
    arbs_both_filled: int = 0
    arbs_one_leg: int = 0
    arbs_neither_filled: int = 0
    unwinds_attempted: int = 0
    unwinds_succeeded: int = 0
    total_edge_captured: float = 0.0
    errors: int = 0


class ArbExecutor:
    """
    Arbitrage execution engine for Polymarket BTC 15m binary markets.

    Registers as a PriceCallback. On each best_bid_ask event:
      1. Fast-path reject if not best_bid_ask
      2. Read both asks from ingestor.state
      3. Lookup-table arb check
      4. If profitable and not in cooldown: spawn execution task
    """

    def __init__(
        self,
        auth: "PolyAuth",
        ingestor: "BtcMarketIngestor",
        http_session: aiohttp.ClientSession,
        mode: Mode = Mode.DRY_RUN,
        max_size: float = 20.0,
        min_edge_bps: float = 0.0,
        cooldown_s: float = 2.0,
        proxy_url: str = "",
        db: "Optional[SupabaseWriter]" = None,
    ) -> None:
        self._auth = auth
        self._ingestor = ingestor
        self._session = http_session
        self._mode = mode
        self._max_size = max_size
        self._min_edge_bps = min_edge_bps
        self._cooldown_s = cooldown_s
        self._proxy_url = proxy_url or None  # None = no proxy
        self._db = db

        if self._proxy_url:
            logger.info("Proxy enabled for CLOB orders: %s", self._proxy_url.split("@")[-1])

        # State tracking
        self._last_attempt_ns: int = 0
        self._generation: int = 0
        self._current_up_token: str = ingestor.state.up.token_id
        self._current_down_token: str = ingestor.state.down.token_id
        self._in_flight: bool = False

        # Statistics and history
        self.stats = ExecutorStats()
        self.history: list[ArbAttempt] = []

        # Position tracking (net shares per token_id)
        self._positions: dict[str, float] = {}

    # ------------------------------------------------------------------
    # PriceCallback interface
    # ------------------------------------------------------------------

    async def __call__(self, event: PriceEvent) -> None:
        """
        PriceCallback entry point. HOT PATH — must return fast.
        No allocations, no I/O, no awaits (except create_task).
        """
        # Fast reject: only act on best_bid_ask events
        if event.event_type != "best_bid_ask":
            return

        # Read current state from the ingestor (picks up rotation automatically)
        state = self._ingestor.state

        # Detect market rotation
        if (
            state.up.token_id != self._current_up_token
            or state.down.token_id != self._current_down_token
        ):
            self._on_rotation(state)

        # Need both asks
        up_ask = state.up.best_ask
        down_ask = state.down.best_ask
        if up_ask is None or down_ask is None:
            return

        # Lookup-table arb check
        if not is_arb(up_ask, down_ask):
            return

        self.stats.arbs_detected += 1
        current_edge = edge(up_ask, down_ask)

        # Minimum edge filter
        edge_bps = current_edge * 10_000
        if edge_bps < self._min_edge_bps:
            return

        # Cooldown check
        now_ns = time.monotonic_ns()
        if now_ns - self._last_attempt_ns < self._cooldown_s * 1_000_000_000:
            return

        # Don't stack concurrent arb attempts
        if self._in_flight:
            return

        # Fire and forget — execution runs concurrently
        self._last_attempt_ns = now_ns
        self._in_flight = True
        generation = self._generation

        asyncio.create_task(
            self._execute_arb(
                up_ask=up_ask,
                down_ask=down_ask,
                up_token_id=self._current_up_token,
                down_token_id=self._current_down_token,
                edge_bps=edge_bps,
                generation=generation,
            )
        )

    # ------------------------------------------------------------------
    # Execution (cold path — async task)
    # ------------------------------------------------------------------

    async def _execute_arb(
        self,
        up_ask: float,
        down_ask: float,
        up_token_id: str,
        down_token_id: str,
        edge_bps: float,
        generation: int,
    ) -> None:
        """Execute an arbitrage: buy both sides concurrently."""
        t_start = time.monotonic()
        try:
            # Polymarket min order = $1.00. Ensure price * size >= 1.0 for both legs.
            min_price = min(up_ask, down_ask)
            min_size_for_dollar = math.ceil(1.0 / min_price) if min_price > 0 else self._max_size
            size = max(self._max_size, float(min_size_for_dollar))
            if size > self._max_size:
                logger.info("Size bumped %d → %d to meet $1 minimum (min ask=%.2f)", int(self._max_size), int(size), min_price)

            attempt = ArbAttempt(
                timestamp=time.time(),
                up_ask=up_ask,
                down_ask=down_ask,
                edge_bps=edge_bps,
                size=size,
                generation=generation,
                mode=self._mode.name,
            )

            logger.info(
                "ARB DETECTED: Up ask=%.4f + Down ask=%.4f = %.4f "
                "(edge=%.1f bps, size=%.1f) [gen=%d]",
                up_ask,
                down_ask,
                up_ask + down_ask,
                edge_bps,
                size,
                generation,
            )

            if self._mode == Mode.DRY_RUN:
                logger.info("DRY RUN — skipping order submission")
                self.stats.arbs_attempted += 1
                attempt.total_duration_ms = (time.monotonic() - t_start) * 1000
                self._record_attempt(attempt)
                return

            # Sign both orders concurrently (CPU-bound, ~1-2ms each)
            t_sign = time.monotonic()
            loop = asyncio.get_event_loop()
            signed_up, signed_down = await asyncio.gather(
                loop.run_in_executor(
                    None,
                    self._auth.create_signed_order,
                    up_token_id,
                    up_ask,
                    size,
                    "BUY",
                    TICK_SIZE,
                    NEG_RISK,
                ),
                loop.run_in_executor(
                    None,
                    self._auth.create_signed_order,
                    down_token_id,
                    down_ask,
                    size,
                    "BUY",
                    TICK_SIZE,
                    NEG_RISK,
                ),
            )
            attempt.sign_duration_ms = (time.monotonic() - t_sign) * 1000

            # Abort if market rotated while signing
            if self._generation != generation:
                logger.warning("Market rotated during signing — aborting")
                attempt.total_duration_ms = (time.monotonic() - t_start) * 1000
                self._record_attempt(attempt)
                return

            self.stats.arbs_attempted += 1

            # POST both orders concurrently
            up_result, down_result = await asyncio.gather(
                self._post_order(signed_up, "Up", up_token_id, up_ask, size),
                self._post_order(signed_down, "Down", down_token_id, down_ask, size),
            )

            attempt.up_result = up_result
            attempt.down_result = down_result
            attempt.total_duration_ms = (time.monotonic() - t_start) * 1000
            self._record_attempt(attempt)

            # Evaluate outcome
            if attempt.both_filled:
                self.stats.arbs_both_filled += 1
                self.stats.total_edge_captured += edge_bps / 10_000 * size
                logger.info(
                    "BOTH LEGS FILLED — edge captured: %.4f USD",
                    edge_bps / 10_000 * size,
                )
                self._positions[up_token_id] = (
                    self._positions.get(up_token_id, 0.0) + size
                )
                self._positions[down_token_id] = (
                    self._positions.get(down_token_id, 0.0) + size
                )
            elif attempt.one_leg_only:
                self.stats.arbs_one_leg += 1
                filled = up_result if up_result.success else down_result
                logger.warning(
                    "ONE LEG ONLY: %s filled, other failed — scheduling unwind",
                    filled.side_label,
                )
                self._positions[filled.token_id] = (
                    self._positions.get(filled.token_id, 0.0) + size
                )
                if self._generation == generation:
                    asyncio.create_task(self._unwind(filled, generation))
            else:
                self.stats.arbs_neither_filled += 1
                logger.info("Neither leg filled (both rejected/failed)")

        except Exception as exc:
            self.stats.errors += 1
            logger.error("Arb execution error: %s", exc, exc_info=True)
        finally:
            self._in_flight = False

    async def _post_order(
        self,
        signed_order: dict,
        side_label: str,
        token_id: str,
        price: float,
        size: float,
    ) -> LegResult:
        """POST a signed order to the CLOB via aiohttp (async)."""
        t0 = time.monotonic()
        body: str = ""
        try:
            body = self._serialize_order(signed_order, order_type="FAK")

            headers = self._auth.build_l2_headers("POST", POST_ORDER_PATH, body)
            headers["Content-Type"] = "application/json"

            url = f"{CLOB_HOST}{POST_ORDER_PATH}"

            async with self._session.post(url, data=body, headers=headers, proxy=self._proxy_url) as resp:
                resp_data = await resp.json()
                latency = (time.monotonic() - t0) * 1000

                if resp.status == 200 and resp_data.get("success"):
                    return LegResult(
                        side_label=side_label,
                        token_id=token_id,
                        price=price,
                        size=size,
                        success=True,
                        order_id=resp_data.get("orderID"),
                        status=resp_data.get("status"),
                        http_status=resp.status,
                        response_body=resp_data,
                        request_body=body,
                        post_latency_ms=round(latency, 2),
                    )
                else:
                    error_msg = resp_data.get("errorMsg", f"HTTP {resp.status}")
                    logger.warning(
                        "Order rejected [%s]: %s", side_label, error_msg
                    )
                    return LegResult(
                        side_label=side_label,
                        token_id=token_id,
                        price=price,
                        size=size,
                        success=False,
                        error=error_msg,
                        http_status=resp.status,
                        response_body=resp_data,
                        request_body=body,
                        post_latency_ms=round(latency, 2),
                    )

        except Exception as exc:
            latency = (time.monotonic() - t0) * 1000
            logger.error("POST error [%s]: %s", side_label, exc)
            return LegResult(
                side_label=side_label,
                token_id=token_id,
                price=price,
                size=size,
                success=False,
                error=str(exc),
                request_body=body or None,
                post_latency_ms=round(latency, 2),
            )

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    def _record_attempt(self, attempt: ArbAttempt) -> None:
        """Append to in-memory history and fire async DB write."""
        self.history.append(attempt)
        if self._db is not None:
            self._db.fire(self._attempt_to_row(attempt))

    @staticmethod
    def _attempt_to_row(a: ArbAttempt) -> dict:
        """Flatten ArbAttempt + LegResults into a single DB row dict."""
        if a.up_result is None and a.down_result is None:
            status = "dry_run"
        elif a.both_filled:
            status = "both_filled"
        elif a.one_leg_only:
            status = "one_leg"
        elif (a.up_result and not a.up_result.success
              and a.down_result and not a.down_result.success):
            status = "neither_filled"
        else:
            status = "error"

        row: dict = {
            "ts": a.timestamp,
            "up_ask": a.up_ask,
            "down_ask": a.down_ask,
            "edge_bps": round(a.edge_bps, 2),
            "size": a.size,
            "generation": a.generation,
            "mode": a.mode,
            "status": status,
            "sign_duration_ms": a.sign_duration_ms,
            "total_duration_ms": a.total_duration_ms,
        }

        for prefix, leg in [("up_", a.up_result), ("down_", a.down_result)]:
            if leg is not None:
                row[f"{prefix}side_label"] = leg.side_label
                row[f"{prefix}token_id"] = leg.token_id
                row[f"{prefix}price"] = leg.price
                row[f"{prefix}size"] = leg.size
                row[f"{prefix}success"] = leg.success
                row[f"{prefix}order_id"] = leg.order_id
                row[f"{prefix}status"] = leg.status
                row[f"{prefix}error"] = leg.error
                row[f"{prefix}http_status"] = leg.http_status
                row[f"{prefix}response_body"] = leg.response_body
                row[f"{prefix}request_body"] = leg.request_body
                row[f"{prefix}post_latency_ms"] = leg.post_latency_ms

        return row

    def _serialize_order(self, signed_order: dict, order_type: str = "FAK") -> str:
        """Serialize signed order for POST /order. Compact JSON to match HMAC body."""
        payload = {
            "order": signed_order,
            "owner": self._auth.api_key,
            "orderType": order_type,
        }
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    # ------------------------------------------------------------------
    # Unwind logic (leg risk management)
    # ------------------------------------------------------------------

    async def _unwind(self, filled_leg: LegResult, generation: int) -> None:
        """Sell the filled side to close one-leg exposure."""
        try:
            self.stats.unwinds_attempted += 1

            if self._generation != generation:
                logger.warning(
                    "Market rotated — cannot unwind %s", filled_leg.side_label
                )
                return

            state = self._ingestor.state
            token = state.by_token(filled_leg.token_id)
            if token is None or token.best_bid is None:
                logger.warning(
                    "Cannot unwind: no bid data for %s", filled_leg.side_label
                )
                return

            sell_price = token.best_bid
            sell_size = filled_leg.size

            logger.info(
                "UNWIND: Selling %.1f %s at %.4f",
                sell_size,
                filled_leg.side_label,
                sell_price,
            )

            if self._mode == Mode.DRY_RUN:
                logger.info("DRY RUN — skipping unwind")
                return

            loop = asyncio.get_event_loop()
            signed = await loop.run_in_executor(
                None,
                self._auth.create_signed_order,
                filled_leg.token_id,
                sell_price,
                sell_size,
                "SELL",
                TICK_SIZE,
                NEG_RISK,
            )

            result = await self._post_order(
                signed,
                f"{filled_leg.side_label}-UNWIND",
                filled_leg.token_id,
                sell_price,
                sell_size,
            )

            if result.success:
                self.stats.unwinds_succeeded += 1
                self._positions[filled_leg.token_id] = (
                    self._positions.get(filled_leg.token_id, 0.0) - sell_size
                )
                logger.info("Unwind succeeded for %s", filled_leg.side_label)
            else:
                logger.error(
                    "Unwind FAILED for %s: %s",
                    filled_leg.side_label,
                    result.error,
                )

        except Exception as exc:
            self.stats.errors += 1
            logger.error("Unwind error: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # Market rotation
    # ------------------------------------------------------------------

    def _on_rotation(self, state) -> None:
        """Handle market rotation: update tracked token IDs, bump generation."""
        old_up = self._current_up_token
        self._current_up_token = state.up.token_id
        self._current_down_token = state.down.token_id
        self._generation += 1
        logger.info(
            "Executor detected market rotation (gen %d): %s... -> %s...",
            self._generation,
            old_up[:12],
            self._current_up_token[:12],
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def proxy_url(self) -> str | None:
        return self._proxy_url

    @property
    def positions(self) -> dict[str, float]:
        return dict(self._positions)

    def summary(self) -> str:
        s = self.stats
        return (
            f"Arbs: detected={s.arbs_detected} attempted={s.arbs_attempted} "
            f"both_filled={s.arbs_both_filled} one_leg={s.arbs_one_leg} "
            f"neither={s.arbs_neither_filled} | "
            f"Unwinds: {s.unwinds_succeeded}/{s.unwinds_attempted} | "
            f"Edge captured: ${s.total_edge_captured:.4f} | "
            f"Errors: {s.errors}"
        )
