import os
import logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# ────────────────────────────────────────────────────────────────────────────
API_KEY         = os.environ["POLYGON_API_KEY"]
MAPPING_FILE    = "T2DPulse_ticker_sector_mapping.txt"
DAYS_HISTORY    = 30
OUT_DIR         = "data"
PER_TICKER_FILE = os.path.join(OUT_DIR, "ticker_market_caps.parquet")
SECTOR_FILE     = os.path.join(OUT_DIR, "sector_history.parquet")

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
def load_mapping(path: str = MAPPING_FILE) -> dict[str, list[str]]:
    """Return {sector: [tickers]} from the CSV mapping."""
    df = pd.read_csv(path)
    return df.groupby("Sector")["Ticker"].apply(list).to_dict()


def fetch_market_caps(ticker: str,
                      start: str,
                      end: str,
                      client: RESTClient) -> pd.Series:
    """
    Return a Series (index=date, value=market_cap) for a single ticker.

    Rules (NO close*volume fallback):
        1. Use Polygon `get_ticker_details` → market_cap if present
        2. Else compute close × shares_outstanding (weighted if available)
        3. Otherwise skip (empty Series)
    """
    # —─ 1️⃣ details snapshot ───────────────────────────────────────────────
    try:
        details = client.get_ticker_details(ticker)
        latest_mcap   = details.market_cap
        shares_out    = details.weighted_shares_outstanding or details.shares_outstanding
    except Exception as exc:
        log.debug("%s details call failed: %s", ticker, exc)
        latest_mcap = shares_out = None

    # —─ 2️⃣ Historical prices (for close × shares) ─────────────────────────
    try:
        bars = client.get_aggs(ticker, 1, "day", start, end, limit=5000)
    except Exception as exc:
        log.warning("Aggs call failed for %s: %s", ticker, exc)
        return pd.Series(dtype="float64")

    if not bars:
        return pd.Series(dtype="float64")

    rows = []
    for bar in bars:
        close = getattr(bar, "c", None) or bar.get("c")
        ts    = getattr(bar, "t", None) or bar.get("t")
        if ts is None:
            continue
        day = pd.to_datetime(ts, unit="ms").date()

        if latest_mcap:                         # case 1
            rows.append((day, latest_mcap))
        elif close and shares_out:              # case 2
            rows.append((day, close * shares_out))
        # else → skip (no market‑cap info available)

    return pd.Series(dict(rows)).sort_index()


# ────────────────────────────────────────────────────────────────────────────
def build_histories():
    client = RESTClient(API_KEY)
    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=DAYS_HISTORY * 2)   # buffer for weekends

    mapping       = load_mapping()
    ticker_frames = []
    sector_series = []

    for sector, tickers in mapping.items():
        log.info("• Sector %-18s (%d tickers)", sector, len(tickers))
        sec_df = pd.DataFrame()

        for tk in tickers:
            s = fetch_market_caps(tk, start.isoformat(), end.isoformat(), client)
            if s.empty:
                log.warning("  ⚠️  %s skipped – no valid market‑cap data", tk)
                continue
            sec_df[tk] = s

        if sec_df.empty:
            log.warning("  ⚠️  sector '%s' produced no rows", sector)
            continue

        # business‑day alignment
        bdays = pd.date_range(end - timedelta(days=DAYS_HISTORY * 2),
                              end, freq="B").date
        sec_df = sec_df.reindex(bdays)

        ticker_frames.append(sec_df)
        sector_series.append(sec_df.sum(axis=1).rename(sector))

    if ticker_frames:
        ticker_history = pd.concat(ticker_frames, axis=1).tail(DAYS_HISTORY)
        ticker_history.to_parquet(PER_TICKER_FILE)
        log.info("✔ wrote %s  (%s × %s)", PER_TICKER_FILE, *ticker_history.shape)

    if sector_series:
        sector_history = pd.concat(sector_series, axis=1).tail(DAYS_HISTORY)
        sector_history.to_parquet(SECTOR_FILE)
        log.info("✔ wrote %s  (%s × %s)", SECTOR_FILE, *sector_history.shape)
    else:
        log.error("‼ No sector data built – nothing written")


# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.makedirs(OUT_DIR, exist_ok=True)
    build_histories()
