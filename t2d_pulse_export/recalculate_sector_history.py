import os
import pandas as pd
from polygon import RESTClient  # polygon.io Python SDK

API_KEY = os.environ.get("POLYGON_API_KEY")  # Set this in Render’s ENV

def load_mapping(path="T2DPulse_ticker_sector_mapping.txt"):
    # Reads your comma-delimited text as CSV
    df = pd.read_csv(path)
    return { sector: group["Ticker"].tolist()
             for sector, group in df.groupby("Sector") }

from polygon import RESTClient
import pandas as pd
import os

API_KEY = os.environ["POLYGON_API_KEY"]

def fetch_market_caps(tickers, start, end):
    """Fetch daily market‐cap estimates (close × volume) for each ticker."""
    client = RESTClient(API_KEY)
    all_data = {}

    for symbol in tickers:
        # Positional args: ticker, multiplier, timespan, _from, to
        bars = client.get_aggs(
            symbol,     # 1st positional arg
            1,          # multiplier
            "day",      # timespan
            start,      # _from (YYYY-MM-DD)
            end,        # to   (YYYY-MM-DD)
            unadjusted=False  # this keyword is accepted
        )

        df = pd.DataFrame(bars)
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
        df.set_index("date", inplace=True)
        all_data[symbol] = df["c"] * df["v"]

    return pd.DataFrame(all_data)

def build_sector_history(mapping, start, end):
    dates = pd.date_range(start, end, freq="B")  # business days
    sector_dfs = []
    for sector, tickers in mapping.items():
        df = fetch_market_caps(tickers, start.isoformat(), end.isoformat())
        total = df.reindex(dates).sum(axis=1).rename(sector)
        sector_dfs.append(total)
    return pd.concat(sector_dfs, axis=1)

if __name__ == "__main__":
    from datetime import date, timedelta

    # Define your period (last 365 days)
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=365)

    # Make sure paths are relative to this script
    os.chdir(os.path.dirname(__file__))

    mapping = load_mapping()
    history = build_sector_history(mapping, start, end)
    history.to_parquet("data/sector_history.parquet")
    print("✔ Wrote updated sector_history.parquet")
