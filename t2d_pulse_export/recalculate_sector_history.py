import os
import json
import pandas as pd
from polygon import RESTClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
try:
    API_KEY = os.environ["POLYGON_API_KEY"]
except KeyError:
    logger.error("POLYGON_API_KEY is not set in environment variables.")
    API_KEY = None

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MAPPING_FILE = os.path.join(DATA_DIR, "sector_ticker_mapping.json")
OUTPUT_PATH = os.path.join(DATA_DIR, "sector_history.parquet")


def load_mapping(path=MAPPING_FILE):
    """
    Load the sector-to-ticker mapping from a JSON file.
    JSON shape: {"Sector Name": ["TICKER1", ...], ...}
    """
    logger.info(f"Loading sector mapping from {path}")
    with open(path, "r") as f:
        mapping = json.load(f)
    logger.info(f"Loaded mapping for {len(mapping)} sectors")
    return mapping


def fetch_market_caps(tickers, start, end):
    """
    Fetch daily market-cap estimates directly from Polygon's ticker details.
    Uses the 'market_cap' field from Polygon's reference endpoint for each symbol.
    Since historical market_cap isn't provided, this will use the latest market cap
    for all dates in the range (or you can fetch snapshots per date if available).
    Returns a DataFrame indexed by date with tickers as columns and constant market cap values.
    """
    client = RESTClient(API_KEY)
    all_series = {}

    # Determine business-day dates
    dates = pd.date_range(start, end, freq="B").date

    for symbol in tickers:
        # Fetch ticker overview with market cap
        try:
            details = client.reference_ticker_details(symbol)
            market_cap_val = getattr(details, "market_cap", None)
            if market_cap_val is None:
                logger.warning(f"No 'market_cap' field for {symbol}; skipping")
                continue
        except Exception as e:
            logger.warning(f"Failed to fetch ticker overview for {symbol}: {e}")
            continue

        # Create a constant series over the date range
        series = pd.Series(market_cap_val, index=dates, name=symbol)
        all_series[symbol] = series
        logger.info(f"Loaded market cap for {symbol}: {market_cap_val}")

    if not all_series:
        logger.error("No market cap series collected; cannot build history.")
        return pd.DataFrame()

    return pd.DataFrame(all_series)(all_series)


def build_sector_history(mapping, start, end):
    """
    Build a DataFrame of daily sector market caps over business days
    by summing individual ticker market caps.
    Returns DataFrame indexed by date with sectors as columns.
    """
    dates = pd.date_range(start, end, freq="B")
    sector_frames = []

    for sector, tickers in mapping.items():
        logger.info(f"Building history for sector '{sector}' with {len(tickers)} tickers")
        df = fetch_market_caps(tickers, start, end)
        if df.empty:
            logger.warning(f"Empty market cap data for sector '{sector}'")
            continue
        totals = df.reindex(dates).sum(axis=1).rename(sector)
        sector_frames.append(totals)

    if not sector_frames:
        logger.error("No sector data frames created; nothing to merge")
        return pd.DataFrame()

    history = pd.concat(sector_frames, axis=1)
    return history


if __name__ == "__main__":
    from datetime import date, timedelta

    # Define the 30-day business window ending yesterday
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=45)

    # Ensure working directory
    os.chdir(os.path.dirname(__file__))

    # Execute rebuild
    mapping = load_mapping()
    sector_history = build_sector_history(mapping, start.isoformat(), end.isoformat())
    if not sector_history.empty:
        sector_history.to_parquet(OUTPUT_PATH)
        logger.info(f"Wrote updated sector_history.parquet at {OUTPUT_PATH}")
    else:
        logger.error("sector_history DataFrame is empty; aborted write")
