#!/usr/bin/env python3
"""
Rebuild the rolling 30‑day sector market‑cap history and save it to
t2d_pulse_export/data/sector_history.parquet.

Prereqs
-------
* POLYGON_API_KEY set in the environment (Render → Environment tab)
* t2d_pulse_export/data/shares.json
    • keys  : ticker symbols (upper‑case)
    • values: *millions* of shares outstanding  (plain integers)
* t2d_pulse_export/data/ticker_sector_map.json
    • keys  : ticker symbols
    • values: sector name (one of the 14 T2D Pulse sectors)

The script:
1. Builds a clean 30‑day window that never points “from” a date after “to”.
2. Fetches adjusted daily close prices from Polygon.
3. Multiplies price × shares_outstanding to get daily market‑cap.
4. Sums market‑cap by sector for every day in the window.
5. Writes the result to Parquet.

If **any** ticker is missing a share count or sector mapping, or an API call
fails, the script exits with a clear fatal error so CI fails fast.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from polygon import RESTClient

# ---------------------------------------------------------------------------
# Configuration paths
ROOT = pathlib.Path(__file__).parent
DATA_DIR = ROOT / "data"

SHARES_JSON = DATA_DIR / "shares.json"               # millions of shares
SECTOR_MAP_JSON = DATA_DIR / "ticker_sector_map.json"
OUTPUT_PARQUET = DATA_DIR / "sector_history.parquet"
WINDOW_DAYS = 30

# ---------------------------------------------------------------------------
# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sector-history")

# ---------------------------------------------------------------------------
# Date helpers
def last_trading_day(ref: date) -> date:
    """Return the most recent weekday ≤ `ref` (Fri if today is Sat/Sun)."""
    while ref.weekday() >= 5:  # 5 = Sat, 6 = Sun
        ref -= timedelta(days=1)
    return ref


def price_window(days: int = WINDOW_DAYS) -> Tuple[str, str]:
    """
    Return (start_iso, end_iso) spanning the most recent *completed*
    trading day and `days`‑1 calendar days before that, guaranteed start < end.
    """
    end_dt = last_trading_day(date.today() - timedelta(days=1))  # yesterday or Fri
    start_dt = end_dt - timedelta(days=days - 1)
    return start_dt.isoformat(), end_dt.isoformat()


# ---------------------------------------------------------------------------
# I/O helpers
def load_json(path: pathlib.Path, desc: str) -> Dict:
    try:
        with path.open() as f:
            return json.load(f)
    except FileNotFoundError:
        sys.exit(f"❌  Missing {desc} at {path}")
    except json.JSONDecodeError as e:
        sys.exit(f"❌  Malformed {desc}: {e}")


def fetch_close_prices(
    client: RESTClient, ticker: str, start_iso: str, end_iso: str
) -> List[Tuple[str, float]]:
    """Return list of (ISO‑date, close_price)."""
    try:
        bars = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=start_iso,
            to=end_iso,
            adjusted=True,
        )
    except Exception as e:
        raise RuntimeError(f"Polygon API error for {ticker}: {e}") from e

    if not bars:
        raise RuntimeError(f"No price data returned for {ticker}")

    out: List[Tuple[str, float]] = []
    for bar in bars:
        # Polygon timestamps are ms since epoch UTC
        iso_day = datetime.utcfromtimestamp(bar["t"] / 1000).date().isoformat()
        out.append((iso_day, float(bar["c"])))
    return out


# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Loading static reference data …")
    shares_mil = load_json(SHARES_JSON, "shares.json")          # {ticker: int}
    sector_map = load_json(SECTOR_MAP_JSON, "ticker‑sector map")  # {ticker: sector}

    missing_sector = sorted(set(shares_mil) - set(sector_map))
    if missing_sector:
        sys.exit(
            f"❌  {len(missing_sector)} tickers lack sector mapping: {', '.join(missing_sector)}"
        )

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        sys.exit("❌  POLYGON_API_KEY not set in environment")

    client = RESTClient(api_key)

    start_iso, end_iso = price_window()
    log.info("Building %s‑day history %s → %s", WINDOW_DAYS, start_iso, end_iso)

    # {date_iso: {sector: market_cap}}
    daily_sector_cap: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for idx, ticker in enumerate(sorted(shares_mil), 1):
        sector = sector_map[ticker]
        shares = shares_mil[ticker] * 1_000_000  # convert to absolute shares

        try:
            prices = fetch_close_prices(client, ticker, start_iso, end_iso)
        except RuntimeError as e:
            sys.exit(f"❌  {e}")

        for day_iso, close_px in prices:
            daily_sector_cap[day_iso][sector] += close_px * shares

        if idx % 20 == 0 or idx == len(shares_mil):
            log.info("  processed %3d / %d tickers …", idx, len(shares_mil))

    # -----------------------------------------------------------------------
    log.info("Transforming to DataFrame …")
    df = (
        pd.DataFrame(daily_sector_cap)  # columns = date, rows = sector
        .transpose()                    # rows = date,  cols = sector
        .sort_index()
    )

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET)
    log.info("✅  Wrote %s (%d rows × %d sectors)", OUTPUT_PARQUET, *df.shape)


if __name__ == "__main__":
    main()
