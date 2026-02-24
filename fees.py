"""
fees.py — Precomputed fee lookup for Polymarket BTC 15m markets.

Fee formula: fee_per_share = FEE_RATE * (p * (1 - p)) ^ EXPONENT
Where FEE_RATE = 0.25, EXPONENT = 2 for BTC 15m crypto markets.

Precomputed at import time for all 99 price ticks (0.01 .. 0.99).
Keyed by integer cents (1..99) to avoid float key hazards.
"""

FEE_RATE: float = 0.25
EXPONENT: int = 2

# FEE_FACTOR[cents] = fee per share at price cents/100
FEE_FACTOR: dict[int, float] = {
    cents: FEE_RATE * (cents / 100.0 * (1.0 - cents / 100.0)) ** EXPONENT
    for cents in range(1, 100)
}


def price_to_cents(price: float) -> int:
    """Convert a float price (0.01..0.99) to integer cents (1..99).

    Uses round() to handle floating-point representation issues
    (e.g. 0.57 stored as 0.5699999...).
    """
    return round(price * 100)


def fee_per_share(price: float) -> float:
    """Fee per share at a given price. Single dict lookup."""
    return FEE_FACTOR.get(price_to_cents(price), 0.0)


def total_cost(ask_up: float, ask_down: float) -> float:
    """Total cost to buy 1 share of each side (asks + fees).

    If < 1.0 there is an arbitrage opportunity.
    """
    return (
        ask_up
        + ask_down
        + FEE_FACTOR.get(price_to_cents(ask_up), 0.0)
        + FEE_FACTOR.get(price_to_cents(ask_down), 0.0)
    )


def is_arb(ask_up: float, ask_down: float) -> bool:
    """Check if buying both sides at current asks is profitable after fees."""
    return total_cost(ask_up, ask_down) < 1.0


def edge(ask_up: float, ask_down: float) -> float:
    """Profit per share pair. Positive = profitable."""
    return 1.0 - total_cost(ask_up, ask_down)
