#!/usr/bin/env python3
"""
fetch_shares.py
--------------------------------
Pull *fully‑diluted* share‑count data for every ticker in
T2DPulse_ticker_sector_mapping.txt using the Polygon.io API and
write the results to data/shares.json.

Requires:
    • polygon-api-client >= 1.14.5
    • A POLYGON_API_KEY environment variable
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
from polygon import RESTClient, exceptions

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    sys.exit("❌  POLYGON_API_KEY is not set")

MAPPING_FILE = Path(__file__).with_name("T2DPulse_ticker_sector_mapping.txt")
OUTPUT_PATH  = Path(__file__).with_name("data") / "shares.json"
OUTPUT_PATH.parent.mkdir(exist_ok=True, parents=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_unique_tickers(mapping_path: Path) -> List[str]:
    """Read mapping file and return a unique ticker list."""
    df = pd.read_csv(mapping_path, usecols=["Ticker"])
    return sorted(df["Ticker"].unique())

def fetch_fds_shares(client: RESTClient, symbol: str, retries: int = 5) -> int | None:
    """
    Get fully‑diluted shares outstanding for `symbol`.
    Retries with exponential back‑off on rate‑limit (429) errors.
    """
    delay = 1.0
    for _ in range(retries):
        try:
            details = client.get_ticker_details(symbol)
            shares = details.get("share_class_shares_outstanding") or details.get(
                "weighted_shares_outstanding"
            )
            return int(shares) if shares else None
        except exceptions.APIException as e:
            if e.status == 429:        # rate‑limit
                time.sleep(delay)
                delay *= 2
                continue
            # Any other API error → bubble up
            raise
    return None

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    client = RESTClient(API_KEY)
    tickers = load_unique_tickers(MAPPING_FILE)

    shares_lookup: Dict[str, int] = {}
    missing: List[str] = []

    print(f"Fetching shares for {len(tickers)} tickers …\n")

    for i, ticker in enumerate(tickers, start=1):
        try:
            shares = fetch_fds_shares(client, ticker)
            if shares:
                shares_lookup[ticker] = shares
                print(f"✓ {ticker:<6} {shares:>15,}")
            else:
                missing.append(ticker)
                print(f"⚠︎ {ticker:<6} (no share data)")
        except Exception as err:
            missing.append(ticker)
            print(f"⚠︎ {ticker:<6} error → {err}")

    # Write JSON
    with open(OUTPUT_PATH, "w") as fp:
        json.dump(shares_lookup, fp, indent=2)

    # Summary
    print("\n———————————————————————————————————————————")
    print(f"✔ wrote {len(shares_lookup):,} tickers → {OUTPUT_PATH}")
    if missing:
        print(f"⚠︎ missing data for {len(missing)} tickers: {', '.join(missing)}")

if __name__ == "__main__":
    main()
