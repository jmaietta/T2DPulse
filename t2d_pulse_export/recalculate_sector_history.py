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
    Fetch daily market-cap for each ticker (close price × fully diluted shares).
    Uses Polygon's ticker details to get outstanding shares, then daily aggregates for price.
    Returns a DataFrame indexed by date, columns=tickers with market cap values.
    """
    client = RESTClient(API_KEY)
    all_series = {}

    for symbol in tickers:
        # Get share count
        try:
            details = client.reference_ticker_details(symbol)
            shares_outstanding = getattr(details, 'share_class_shares_outstanding', None) or getattr(details, 'outstanding_shares', None)
            if not shares_outstanding:
                logger.warning(f"No outstanding shares data for {symbol}; skipping")
                continue
        except Exception as e:
            logger.warning(f"Failed to fetch ticker details for {symbol}: {e}")
            continue

        # Fetch daily price aggregates
        try:
            bars = client.get_aggs(symbol, 1, "day", start, end)
        except Exception as e:
            logger.warning(f"API error fetching aggregates for {symbol}: {e}")
            continue
        if not bars:
            logger.warning(f"No aggregate data for {symbol}")
            continue

        records = []
        for bar in bars:
            price = getattr(bar, 'c', None)
            ts = getattr(bar, 't', None)
            if price is None or ts is None:
                continue
            date = pd.to_datetime(ts, unit='ms').date()
            records.append({'date': date, 'price': price})

        if not records:
            logger.warning(f"No valid price records for {symbol}")
            continue

        df = pd.DataFrame(records).set_index('date')
        # Calculate market cap
        df['market_cap'] = df['price'] * shares_outstanding
        all_series[symbol] = df['market_cap']
        logger.info(f"Processed {len(df)} days for {symbol}")

    if not all_series:
        logger.error("No market cap series collected for any tickers")
    return pd.DataFrame(all_series)


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
