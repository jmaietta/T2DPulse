import os
import json
import logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths and configuration
data_dir = os.path.join(os.path.dirname(__file__), "data")
mapping_file = os.path.join(data_dir, "sector_ticker_mapping.json")
output_file = os.path.join(data_dir, "sector_history.parquet")
API_KEY = os.getenv("POLYGON_API_KEY")

# Date range for 30 trading days (approx. 45 calendar days back)
end_date = date.today() - timedelta(days=1)
start_date = end_date - timedelta(days=45)


def load_mapping(path=mapping_file):
    """
    Load sector-to-ticker mapping from JSON.
    Returns dict: {sector: [tickers]}
    """
    logger.info(f"Loading sector mapping from {path}")
    with open(path, 'r') as f:
        mapping = json.load(f)
    logger.info(f"Loaded mapping for {len(mapping)} sectors")
    return mapping


def fetch_ticker_market_cap(symbol, start, end):
    """
    Fetch daily market cap for a single ticker over the date range.
    Market cap = closing price × fully diluted shares outstanding.
    Returns a Series indexed by date.
    """
    client = RESTClient(API_KEY)
    # 1) Fetch outstanding shares
    try:
        details = client.reference_ticker_details(symbol)
        shares = (getattr(details, 'share_class_shares_outstanding', None)
                  or getattr(details, 'outstanding_shares', None))
        if not shares:
            logger.warning(f"No share count for {symbol}")
            return pd.Series(dtype=float)
    except Exception as e:
        logger.error(f"Error fetching share count for {symbol}: {e}")
        return pd.Series(dtype=float)

    # 2) Fetch daily close prices
    try:
        bars = client.get_aggs(symbol, 1, 'day', start.isoformat(), end.isoformat())
    except Exception as e:
        logger.error(f"Error fetching price bars for {symbol}: {e}")
        return pd.Series(dtype=float)
    if not bars:
        logger.warning(f"No price data for {symbol}")
        return pd.Series(dtype=float)

    records = []
    for bar in bars:
        dt = pd.to_datetime(bar.t, unit='ms').date()
        price = bar.c
        records.append((dt, price * shares))

    if not records:
        return pd.Series(dtype=float)

    s = pd.Series({dt: mc for dt, mc in records})
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def build_sector_history(mapping, start, end):
    """
    Build a DataFrame of sector market caps over trading days.
    Sums individual ticker market caps per date.
    """
    logger.info(f"Building sector history from {start} to {end}")
    # Determine business days in range
    dates = pd.date_range(start, end, freq='B')
    frames = []

    for sector, tickers in mapping.items():
        logger.info(f"Processing sector '{sector}' with {len(tickers)} tickers")
        ticker_caps = []
        for sym in tickers:
            series = fetch_ticker_market_cap(sym, start, end)
            if not series.empty:
                ticker_caps.append(series)
        if not ticker_caps:
            logger.warning(f"No data for sector '{sector}'")
            continue
        df_t = pd.concat(ticker_caps, axis=1)
        df_t = df_t.reindex(dates).fillna(method='ffill').fillna(0)
        sector_total = df_t.sum(axis=1).rename(sector)
        frames.append(sector_total)

    if not frames:
        logger.error("No sector frames; nothing to write")
        return pd.DataFrame()

    history = pd.concat(frames, axis=1)
    return history


if __name__ == '__main__':
    # Ensure working directory
    os.chdir(os.path.dirname(__file__))

    mapping = load_mapping()
    history = build_sector_history(mapping, start_date, end_date)

    if history.empty:
        logger.error("Sector history is empty; aborting write")
    else:
        history.to_parquet(output_file)
        logger.info(f"Wrote sector history to {output_file}")
