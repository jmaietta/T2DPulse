import os
import json
import logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directory paths
base_dir = os.path.dirname(__file__)
data_dir = os.path.join(base_dir, "data")
mapping_file = os.path.join(data_dir, "sector_ticker_mapping.json")
output_file = os.path.join(data_dir, "sector_history.parquet")

# API key for Polygon
API_KEY = os.getenv("POLYGON_API_KEY")

# Date range: last 30 trading days (~45 calendar days back to ensure coverage)
end_date = date.today() - timedelta(days=1)
start_date = end_date - timedelta(days=45)


def load_mapping(path=mapping_file):
    """
    Load sector-to-ticker mapping from JSON file.
    Returns dict: {sector: [ticker symbols]}
    """
    logger.info(f"Loading sector mapping from {path}")
    with open(path, 'r') as f:
        mapping = json.load(f)
    logger.info(f"Loaded mapping for {len(mapping)} sectors")
    return mapping


def fetch_ticker_market_cap(symbol, start, end):
    """
    Fetch daily market cap for a ticker over the given date range.
    Market cap = closing price × fully diluted shares outstanding.
    Returns a pandas Series indexed by date.
    """
    client = RESTClient(API_KEY)
    try:
        details = client.reference_ticker_details(symbol)
        shares_out = getattr(details, 'share_class_shares_outstanding', None) or getattr(details, 'outstanding_shares', None)
        if not shares_out:
            logger.warning(f"No share count for {symbol}")
            return pd.Series(dtype=float)
    except Exception as e:
        logger.error(f"Failed to fetch share count for {symbol}: {e}")
        return pd.Series(dtype=float)

    try:
        bars = client.get_aggs(symbol, 1, 'day', start.isoformat(), end.isoformat())
    except Exception as e:
        logger.error(f"Failed to fetch price bars for {symbol}: {e}")
        return pd.Series(dtype=float)
    if not bars:
        logger.warning(f"No price bars for {symbol}")
        return pd.Series(dtype=float)

    # Build series of daily market caps
    data = []
    for bar in bars:
        dt = pd.to_datetime(bar.t, unit='ms').date()
        price = bar.c
        mc = price * shares_out
        data.append((dt, mc))
    if not data:
        return pd.Series(dtype=float)

    series = pd.Series({dt: mc for dt, mc in data})
    series.index = pd.to_datetime(series.index)
    return series.sort_index()


def build_sector_history(mapping, start, end):
    """
    Aggregate ticker market caps into sector history over business days.
    Returns a DataFrame indexed by date with sectors as columns.
    """
    dates = pd.date_range(start, end, freq='B')
    sector_frames = []

    for sector, tickers in mapping.items():
        logger.info(f"Building history for sector '{sector}'")
        caps = []
        for ticker in tickers:
            series = fetch_ticker_market_cap(ticker, start, end)
            if not series.empty:
                caps.append(series)
        if not caps:
            logger.warning(f"No data for sector '{sector}'")
            continue
        df_caps = pd.concat(caps, axis=1)
        df_caps = df_caps.reindex(dates).fillna(method='ffill').fillna(0)
        sector_total = df_caps.sum(axis=1).rename(sector)
        sector_frames.append(sector_total)

    if not sector_frames:
        logger.error("No sector data available; aborting.")
        return pd.DataFrame()

    history = pd.concat(sector_frames, axis=1)
    return history


if __name__ == '__main__':
    # Ensure script runs in its folder
    os.chdir(base_dir)

    mapping = load_mapping()
    history = build_sector_history(mapping, start_date, end_date)

    if history.empty:
        logger.error("Sector history DataFrame is empty; no file written.")
    else:
        history.to_parquet(output_file)
        logger.info(f"Wrote updated sector history to {output_file}")
