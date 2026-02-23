"""
auth.py — Polymarket CLOB authentication: L2 credential derivation + HMAC headers.

One-time setup:
  1. Load private key from POLYMARKET_PRIVATE_KEY env var.
  2. Use py-clob-client to derive L2 API credentials (apiKey, secret, passphrase)
     via EIP-712 signed message.
  3. Cache credentials for the session lifetime.

Per-request:
  - Generate HMAC-SHA256 headers for each POST /order call.
  - Sign orders via ClobClient.create_order() (CPU-only, no I/O).
"""

import base64
import hashlib
import hmac
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet


@dataclass(frozen=True)
class ApiCreds:
    """Cached L2 API credentials."""
    api_key: str
    secret: str
    passphrase: str


class PolyAuth:
    """
    Manages Polymarket CLOB authentication lifecycle.

    Instantiate once at startup. Derives L2 creds, then provides:
      - create_signed_order()  (CPU-only EIP-712 signing)
      - build_l2_headers()     (HMAC-SHA256 header generation)
    """

    def __init__(self, private_key: Optional[str] = None) -> None:
        self._private_key = private_key or os.environ["POLYMARKET_PRIVATE_KEY"]
        self._client: Optional[ClobClient] = None
        self._creds: Optional[ApiCreds] = None
        self._address: str = ""

    def setup(self) -> None:
        """
        Derive L2 credentials. Call once at startup (synchronous, ~1-2s).

        Performs an EIP-712 signature + HTTP call to Polymarket to
        create or derive API keys.
        """
        logger.info("Initializing Polymarket auth...")
        self._client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=self._private_key,
            signature_type=0,  # EOA wallet
        )

        raw_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(raw_creds)

        self._creds = ApiCreds(
            api_key=raw_creds.api_key,
            secret=raw_creds.api_secret,
            passphrase=raw_creds.api_passphrase,
        )

        self._address = self._client.get_address()
        logger.info(
            "Auth ready. Address: %s, API key: %s...",
            self._address,
            self._creds.api_key[:8],
        )

    def create_signed_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str = "BUY",
        tick_size: str = "0.01",
        neg_risk: bool = False,
    ) -> dict:
        """
        Build and EIP-712 sign an order. CPU-only, no network I/O.

        Returns the signed order dict ready for POST serialisation.
        """
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY if side == "BUY" else SELL,
        )
        signed = self._client.create_order(
            order_args,
            options=PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )
        # create_order returns a SignedOrder dataclass; convert to plain dict
        # so json.dumps() in executor._serialize_order() works.
        return signed.dict()

    def build_l2_headers(
        self,
        method: str,
        request_path: str,
        body: str = "",
    ) -> dict[str, str]:
        """
        Generate HMAC-SHA256 L2 authentication headers for a CLOB request.

        Returns dict with POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP,
        POLY_API_KEY, POLY_PASSPHRASE.
        """
        timestamp = str(int(time.time()))

        message = timestamp + method + request_path
        if body:
            message += body

        secret_bytes = base64.urlsafe_b64decode(self._creds.secret)
        signature = base64.urlsafe_b64encode(
            hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")

        return {
            "POLY_ADDRESS": self._address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": self._creds.api_key,
            "POLY_PASSPHRASE": self._creds.passphrase,
        }

    @property
    def address(self) -> str:
        return self._address

    @property
    def api_key(self) -> str:
        return self._creds.api_key if self._creds else ""

    @property
    def is_ready(self) -> bool:
        return self._creds is not None
