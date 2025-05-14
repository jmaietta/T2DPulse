import os
import json
import pandas as pd
from polygon import RESTClient

# Configuration
API_KEY = os.environ.get("POLYGON_API_KEY")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MAPPING_FILE = os.path.join(DATA_DIR, "sector_ticker_mapping.json")
OUTPUT_PATH = os.path.join(DATA_DIR, "sector_history.parquet")


def load_mapping(path=MAPPING_FILE):
    """
    Load the sector-to-ticker mapping from a JSON file.
    The JSON should be a dict: {"Sector Name": ["TICKER1", "TICKER2", ...], ...}
    """
    with open(path, "r") as f:
        mapping = json.load(f)
    return mapping


def fetch_market_caps(tickers, start, end):
    """
    Fetch daily market-cap estimates (close price × volume) for each ticker
    between start and end dates (YYYY-MM-DD).
    Returns a DataFrame with dates as index and tickers as columns.
    """
    client = RESTClient(API_KEY)
    all_series = {}

    for symbol in tickers:
        try:
            bars = client.get_aggs(symbol, 1, "day", start, end)
        except Exception:
            continue
        if not bars:
            continue

        # Extract t, c, v fields
        records = []
        for bar in bars:
            t = getattr(bar, "t", None)
            c = getattr(bar, "c", None)
            v = getattr(bar, "v", None)
            if t is None or c is None or v is None:
                continue
            records.append({"t": t, "c": c, "v": v})

        df = pd.DataFrame.from_records(records)
        if df.empty:
            continue

        # Convert epoch ms to date index
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
        df.set_index("date", inplace=True)

        # Market cap = close * volume
        all_series[symbol] = df["c"] * df["v"]

    return pd.DataFrame(all_series)


def build_sector_history(mapping, start, end):
    """
    Build a DataFrame of cumulative sector market caps over business days.
    Returns a DataFrame indexed by date with sectors as columns.
    """
    dates = pd.date_range(start, end, freq="B")  # business days
    sector_frames = []

    for sector, tickers in mapping.items():
        df = fetch_market_caps(tickers, start, end)
        totals = df.reindex(dates).sum(axis=1).rename(sector)
        sector_frames.append(totals)

    history = pd.concat(sector_frames, axis=1)
    return history


if __name__ == "__main__":
    # Calculate the date range: last 365 calendar days up to yesterday
    from datetime import date, timedelta

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=365)

    # Run from script directory
    os.chdir(os.path.dirname(__file__))

    # Load mapping, build history, and write to Parquet
    mapping = load_mapping()
    sector_history = build_sector_history(mapping, start.isoformat(), end.isoformat())
    sector_history.to_parquet(OUTPUT_PATH)

    print(f"✔ Wrote updated sector_history.parquet at {OUTPUT_PATH}")
