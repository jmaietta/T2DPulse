# refresh_macro_data.py
# ETL script: fetch macroeconomic and market indicators and store in Postgres

import os
import datetime as dt
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from pandas_datareader import data as pdr
import yfinance as yf

# --- 1) Database connection ---
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL environment variable not set")
engine = sqlalchemy.create_engine(db_url)

# --- 2) Define series to fetch ---
FRED_SERIES = {
    "CPIAUCSL": "Inflation (CPI)",
    "FEDFUNDS": "Fed Funds Rate",
    "DGS10": "10Y Treasury Yield",
    "USACSCICP02STSAM": "Consumer Sentiment",
    "PCE": "Personal Consumption Expenditures",
    "PCEPI": "PCE Price Index",
    "GDPC1": "Real GDP",
    "PCU511210511210": "Software PPI",
    "PCU5112105112105": "Data Processing PPI",
    "UNRATE": "Unemployment Rate",
    "IHLIDXUSTPSOFTDEVE": "Software Job Postings"
}
YF_INDICES = {
    "^IXIC": "NASDAQ",
    "^VIX": "VIX"
}

# --- 3) Fetch FRED series (last 365 days) ---
end = dt.date.today()
start = end - dt.timedelta(days=365)
for series, name in FRED_SERIES.items():
    # Remove existing rows in date range
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM macro_data WHERE series = :series AND date BETWEEN :start AND :end"),
            {"series": series, "start": start, "end": end}
        )
    try:
        df = pdr.DataReader(
            series,
            "fred",
            start,
            end,
            api_key=os.getenv("FRED_API_KEY")
        )
    except Exception as e:
        print(f"⚠️ Failed to fetch {series} from FRED: {e}")
        continue
    df = df.reset_index()
    df.columns = ['date', 'value']
    df['series'] = series
    # Write to DB
    df.to_sql('macro_data', engine, if_exists='append', index=False, method='multi')
    print(f"✅ Loaded {len(df)} rows for {series}")

# --- 4) Fetch Yahoo Finance indices (last 60 days) ---
end = dt.date.today()
start = end - dt.timedelta(days=60)
for ticker, series in YF_INDICES.items():
    # Remove existing rows in date range
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM macro_data WHERE series = :series AND date BETWEEN :start AND :end"),
            {"series": series, "start": start, "end": end}
        )
    try:
        yf_df = yf.download(ticker, start=start, end=end, progress=False)
    except Exception as e:
        print(f"⚠️ Failed to fetch {ticker} from Yahoo: {e}")
        continue
    yf_df = yf_df.reset_index()[['Date', 'Close']]
    yf_df.columns = ['date', 'value']
    yf_df['series'] = series
    # Write to DB
    yf_df.to_sql('macro_data', engine, if_exists='append', index=False, method='multi')
    print(f"✅ Loaded {len(yf_df)} rows for {series}")

# --- 5) Done ---
print("🌐 Macro data ETL complete.")
