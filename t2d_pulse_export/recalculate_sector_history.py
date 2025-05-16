import os
import json
import logging
from datetime import date, timedelta

import pandas as pd
from polygon import RESTClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths and API Key
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
MAPPING_FILE = os.path.join(DATA_DIR, "sector_ticker_mapping.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "sector_history.parquet")
API_KEY = os.getenv("POLYGON_API_KEY")

# Date range: last 30 trading days (~45 calendar days to ensure coverage)
END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - timedelta(days=45)


def load_mapping(path=MAPPING_FILE):
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
    Fetch daily market cap for a ticker over a date range.
    Market cap = closing price × fully diluted shares outstanding.
    Returns a pandas Series indexed by date.
    """
    client = RESTClient(API_KEY)
    # 1) Get share count
    try:
        # Polygon Python client uses reference_ticker() for ticker details
        details = client.reference_ticker(symbol)
        shares = getattr(details, 'share_class_shares_outstanding', None) or getattr(details, 'outstanding_shares', None)
        if not shares:
            logger.warning(f"No share count for {symbol}")
            return pd.Series(dtype=float)
    except Exception as e:
        logger.error(f"Error fetching details for {symbol}: {e}")
        return pd.Series(dtype=float)

    # 2) Get daily price bars
    try:
        bars = client.get_aggs(symbol, 1, 'day', start.isoformat(), end.isoformat())
    except Exception as e:
        logger.error(f"Error fetching price bars for {symbol}: {e}")
        return pd.Series(dtype=float)
    if not bars:
        logger.warning(f"No price bars for {symbol}")
        return pd.Series(dtype=float)

    # 3) Build market cap series
    records = []
    for bar in bars:
        dt = pd.to_datetime(bar.t, unit='ms').date()
        if bar.c is None:
            continue
        records.append((dt, bar.c * shares))
    if not records:
        return pd.Series(dtype=float)

    series = pd.Series({dt: mc for dt, mc in records})
    series.index = pd.to_datetime(series.index)
    return series.sort_index()


def build_sector_history(mapping, start, end):
    """
    Aggregate ticker market caps into sector history over business days.
    Returns a DataFrame indexed by date with sectors as columns.
    """
    dates = pd.date_range(start, end, freq='B')
    sector_data = []

    for sector, tickers in mapping.items():
        logger.info(f"Building history for sector '{sector}'")
        frames = []
        for ticker in tickers:
            s = fetch_ticker_market_cap(ticker, start, end)
            if not s.empty:
                s = s.reindex(dates).fillna(method='ffill').fillna(0)
                frames.append(s)
        if frames:
            df = pd.concat(frames, axis=1)
            sector_data.append(df.sum(axis=1).rename(sector))
        else:
            logger.warning(f"No data for sector '{sector}'")

    if not sector_data:
        logger.error("No sector data available; aborting.")
        return pd.DataFrame()

    history = pd.concat(sector_data, axis=1)
    history.to_parquet(OUTPUT_FILE)
    logger.info(f"Wrote updated sector history to {OUTPUT_FILE}")
    return history


if __name__ == '__main__':
    os.chdir(BASE_DIR)
    mapping = load_mapping()
    history = build_sector_history(mapping, START_DATE, END_DATE)
    if history.empty:
        logger.error("Sector history DataFrame is empty; no file written.")
    else:
        # For diagnostic: print a sample of the generated history
        print("=== Sector History Sample ===")
        print(history.head().to_string())
        print("=== End Sample ===")
