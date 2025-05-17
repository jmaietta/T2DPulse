#!/usr/bin/env python3
"""
recalculate_sector_history.py
———————————————
• Builds a 30‑trading‑day history of **per‑ticker market‑caps** (EOD close ×
  shares_outstanding if Polygon provides it → falls back to close × volume).
• Aggregates those market‑caps into 14 sector totals.
• Writes TWO parquet files that other parts of the stack (and human QA) can use:

    data/ticker_market_caps.parquet   # rows = dates, columns = tickers
    data/sector_history.parquet       # rows = dates, columns = sectors
"""

import os
import json
import logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
API_KEY           = os.environ["POLYGON_API_KEY"]          # must exist in Render
MAPPING_FILE      = "T2DPulse_ticker_sector_mapping.txt"   # Sector,Ticker CSV
DAYS_OF_HISTORY   = 30                                     # trailing trading days
OUT_DIR           = "data"
PER_TICKER_FILE   = os.path.join(OUT_DIR, "ticker_market_caps.parquet")
SECTOR_FILE       = os.path.join(OUT_DIR, "sector_history.parquet")

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def load_mapping(path: str = MAPPING_FILE) -> dict[str, list[str]]:
    """Sector → [tickers]"""
    df = pd.read_csv(path)
    return df.groupby("Sector")["Ticker"].apply(list).to_dict()

def fetch_market_caps(ticker: str, start: str, end: str, client: RESTClient) -> pd.Series:
    """
    Returns a Series indexed by date with the *market‑cap* for one ticker.
    Logic:
        1. Try Polygon's ‘ticker details’ endpoint for `market_cap` (latest only)
        2. Else build mcap = close × shares_outstanding  (needs details call)
        3. Else fall back to close × volume
    The same value is repeated across the date range if only latest market_cap is available.
    """
    # --- step 1: latest details (single snapshot) ----------------------------
    try:
        details = client.get_ticker_details(ticker)
        mc = details.market_cap
        shares = details.weighted_shares_outstanding or details.shares_outstanding
    except Exception as exc:
        log.debug("Details call failed for %s: %s", ticker, exc)
        details = None
        mc = shares = None

    # --- step 2: historical prices -------------------------------------------
    try:
        bars = client.get_aggs(ticker, 1, "day", start, end, limit=5000)
    except Exception as exc:
        log.warning("Aggs call failed for %s: %s", ticker, exc)
        return pd.Series(dtype="float64")       # empty

    if not bars:                                # no data
        return pd.Series(dtype="float64")

    rows = []
    for bar in bars:
        # bar is Agg or dict; support both
        close  = getattr(bar, "c", None) or bar.get("c")
        volume = getattr(bar, "v", None) or bar.get("v")
        ts     = getattr(bar, "t", None) or bar.get("t")
        if close is None or ts is None:
            continue
        day = pd.to_datetime(ts, unit="ms").date()
        if mc is not None:
            rows.append((day, mc))                       # step 1 value
        elif shares is not None:
            rows.append((day, close * shares))           # step 2 value
        elif volume is not None:
            rows.append((day, close * volume))           # fallback
    if not rows:
        return pd.Series(dtype="float64")

    s = pd.Series(dict(rows)).sort_index()
    return s

# ──────────────────────────────────────────────────────────────────────────────
# Build history
# ──────────────────────────────────────────────────────────────────────────────
def build_histories():
    client = RESTClient(API_KEY)

    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=DAYS_OF_HISTORY * 2)  # give extra for weekends

    mapping = load_mapping()
    all_ticker_frames = []      # for concatenation later
    sector_frames     = []      # list of Series

    for sector, tickers in mapping.items():
        log.info("• Sector %-20s (%d tickers)", sector, len(tickers))
        sector_df = pd.DataFrame()

        for tk in tickers:
            s = fetch_market_caps(tk, start.isoformat(), end.isoformat(), client)
            if s.empty:
                log.warning("  ⚠️  %s -> no data", tk)
                continue
            sector_df[tk] = s

        if sector_df.empty:
            log.warning("  ⚠️  sector '%s' has no data after fetch", sector)
            continue

        # Align to business‑day index (fills missing days with NaN)
        bdays = pd.date_range(end - timedelta(days=DAYS_OF_HISTORY*2),
                              end, freq="B").date
        sector_df = sector_df.reindex(bdays)

        # Keep per‑ticker data for later inspection
        all_ticker_frames.append(sector_df)

        # Sum across tickers → one series per sector
        total = sector_df.sum(axis=1).rename(sector)
        sector_frames.append(total)

    # ---------- Write parquet files ------------------------------------------
    if all_ticker_frames:
        ticker_history = pd.concat(all_ticker_frames, axis=1).tail(DAYS_OF_HISTORY)
        ticker_history.to_parquet(PER_TICKER_FILE)
        log.info("✔ wrote %s  (%s × %s)", PER_TICKER_FILE,
                 *ticker_history.shape)

    if sector_frames:
        sector_history = pd.concat(sector_frames, axis=1).tail(DAYS_OF_HISTORY)
        sector_history.to_parquet(SECTOR_FILE)
        log.info("✔ wrote %s  (%s × %s)", SECTOR_FILE,
                 *sector_history.shape)
    else:
        log.error("‼ No sector data built – nothing written")

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Ensure we run from repo root—Render shell often drops us in /project/src/…
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(OUT_DIR, exist_ok=True)
    build_histories()
