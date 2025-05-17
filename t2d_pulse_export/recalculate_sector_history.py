#!/usr/bin/env python3
"""
Rebuild sector‑level market‑cap history (30 business days) directly from Polygon.
• uses a static shares‑outstanding file:  data/shares.json
• writes ticker‑level prices  -> data/price_history.parquet
• writes sector‑level caps    -> data/sector_history.parquet
"""

import os, json, logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# ──────────────────────────  CONFIG  ────────────────────────── #

API_KEY = os.environ.get("POLYGON_API_KEY")
SHARES_FILE = os.path.join("data", "shares.json")
MAPPING_FILE = os.path.join("data", "sector_ticker_mapping.json")  # {sector: [tickers]}
N_DAYS = 30                                   # business‑day window
OUT_PRICE = os.path.join("data", "price_history.parquet")
OUT_SECTOR = os.path.join("data", "sector_history.parquet")

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s",
)

# ───────────────────────  LOAD STATIC DATA  ─────────────────── #

client = RESTClient(API_KEY, timeout=30)

with open(SHARES_FILE, "r") as f:
    SHARES = json.load(f)                     # {ticker: floats}

with open(MAPPING_FILE, "r") as f:
    SECTOR_MAP = json.load(f)                 # {sector: [tickers]}

end   = date.today() - timedelta(days=1)
start = pd.bdate_range(end, periods=N_DAYS)[0].date()   # first of the 30 BD window
start_iso, end_iso = start.isoformat(), end.isoformat()

# ───────────────────────  HELPER FUNCTIONS  ─────────────────── #

def fetch_close_prices(ticker: str) -> pd.Series:
    """Return a Series(date -> close) for `ticker` over the configured window."""
    bars = client.get_aggs(ticker, 1, "day", start_iso, end_iso, adjusted=True)
    if not bars:
        logging.warning(f"no bars for {ticker}")
        return pd.Series(dtype=float)

    data = {pd.to_datetime(bar.t, unit="ms").date(): bar.c for bar in bars if bar.c is not None}
    return pd.Series(data, name=ticker)


def build_price_history() -> pd.DataFrame:
    """Concatenate Series for every ticker that has a shares entry."""
    frames = []
    for tkr in SHARES.keys():
        s = fetch_close_prices(tkr)
        if not s.empty:
            frames.append(s)

    if not frames:
        raise RuntimeError("no price data retrieved")

    price_df = pd.concat(frames, axis=1).sort_index()          # index = date
    price_df.index = pd.to_datetime(price_df.index)            # ensure DateTimeIndex
    return price_df


def price_to_mcap(price_df: pd.DataFrame) -> pd.DataFrame:
    """Multiply each ticker column by its static shares outstanding."""
    mcap = price_df.copy()
    for tkr in mcap.columns:
        shares = SHARES.get(tkr)
        if shares is None:
            logging.warning(f"missing shares for {tkr}; column dropped")
            mcap.drop(columns=tkr, inplace=True)
            continue
        mcap[tkr] = mcap[tkr] * shares
    return mcap


def aggregate_sectors(mcap_df: pd.DataFrame) -> pd.DataFrame:
    """Sum ticker caps per sector."""
    sector_frames = []
    for sector, tickers in SECTOR_MAP.items():
        cols = [t for t in tickers if t in mcap_df.columns]
        if not cols:
            logging.warning(f"sector {sector} has no valid tickers")
            continue
        sector_frames.append(mcap_df[cols].sum(axis=1).rename(sector))

    if not sector_frames:
        raise RuntimeError("no sector columns built")

    return pd.concat(sector_frames, axis=1)


# ──────────────────────────  MAIN  ──────────────────────────── #

if __name__ == "__main__":
    logging.info(f"Building {N_DAYS}‑day price history {start_iso} → {end_iso}")
    price_hist = build_price_history()
    price_hist.to_parquet(OUT_PRICE)
    logging.info(f"✓ wrote {OUT_PRICE}")

    mcap_hist = price_to_mcap(price_hist)
    sector_hist = aggregate_sectors(mcap_hist)
    sector_hist.to_parquet(OUT_SECTOR)
    logging.info(f"✓ wrote {OUT_SECTOR}  ({sector_hist.shape[0]} rows × {sector_hist.shape[1]} sectors)")
