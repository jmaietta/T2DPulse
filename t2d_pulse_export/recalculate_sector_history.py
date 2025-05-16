import os
import pandas as pd
from polygon import RESTClient
from datetime import date, timedelta

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
    with open(path, 'r') as f:
        return pd.read_json(f, typ='series').to_dict()


def fetch_market_caps(tickers, start, end):
    """
    Fetch estimated daily market cap = close price × volume for each ticker.
    Returns a DataFrame indexed by date with tickers as columns.
    """
    client = RESTClient(API_KEY)
    all_caps = {}
    for symbol in tickers:
        try:
            bars = client.get_aggs(symbol, 1, 'day', start.isoformat(), end.isoformat())
        except Exception:
            continue
        records = []
        for bar in bars:
            if bar.c is None or bar.v is None:
                continue
            date_idx = pd.to_datetime(bar.t, unit='ms').date()
            records.append((date_idx, bar.c * bar.v))
        if not records:
            continue
        ser = pd.Series({dt: cap for dt, cap in records})
        ser.index = pd.to_datetime(ser.index)
        all_caps[symbol] = ser
    return pd.DataFrame(all_caps)


def build_sector_history(mapping, start, end):
    """
    Aggregate ticker market caps into 30-day sector history.
    Returns DataFrame indexed by date with sectors as columns.
    """
    dates = pd.date_range(start, end, freq='B')
    sector_frames = []
    for sector, tickers in mapping.items():
        df = fetch_market_caps(tickers, start, end)
        if df.empty:
            continue
        df = df.reindex(dates).fillna(method='ffill').fillna(0)
        sector_totals = df.sum(axis=1).rename(sector)
        sector_frames.append(sector_totals)
    if not sector_frames:
        raise RuntimeError("No sector data generated")
    history = pd.concat(sector_frames, axis=1)
    history.to_parquet(OUTPUT_FILE)
    print(f"Wrote sector history to {OUTPUT_FILE}")
    return history


if __name__ == '__main__':
    os.chdir(BASE_DIR)
    mapping = load_mapping()
    build_sector_history(mapping, START_DATE, END_DATE)
