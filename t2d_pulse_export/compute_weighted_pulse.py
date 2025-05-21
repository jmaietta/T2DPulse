# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild sector and overall T2D Pulse history using a 3-day EMA for sector sentiment
# calculated via equal-weighted average of stock sentiments, and then compute the Pulse as the
# simple daily average of the 14 sector scores (equal-weight sectors).
# Add a 3-day momentum flag for each sector when sentiment increases/decreases consecutively.
# All data flows through Postgres—no CSVs.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Load stock-level sentiment (3-day EMA) and sector assignments
df_stock = pd.read_sql(
    "SELECT date, ticker, sentiment_score AS stock_sentiment FROM stock_sentiment_history",
    engine
)
df_stock['date'] = pd.to_datetime(df_stock['date']).dt.date

df_sector = pd.read_sql(
    "SELECT DISTINCT ticker, sector FROM market_cap_history",
    engine
)

# 3) Merge and calculate raw sector sentiment (equal-weighted)
df = df_stock.merge(df_sector, on='ticker')
sector_raw = (
    df.groupby(['date','sector'])['stock_sentiment']
      .mean()
      .reset_index(name='sector_sentiment_raw')
)

# 4) Smooth sector sentiment with a 3-day EMA
sector_raw['sector_sentiment_score'] = (
    sector_raw.groupby('sector')['sector_sentiment_raw']
      .transform(lambda x: x.ewm(span=3, adjust=False).mean())
)

# 5) Compute 3-day momentum flag: ↗ if 3-day up, ↘ if 3-day down, — otherwise
sector_raw = sector_raw.sort_values(['sector','date'])
sector_raw['lag1'] = sector_raw.groupby('sector')['sector_sentiment_score'].shift(1)
sector_raw['lag2'] = sector_raw.groupby('sector')['sector_sentiment_score'].shift(2)

def momentum_flag(row):
    if pd.isna(row['lag1']) or pd.isna(row['lag2']):
        return '—'
    if row['sector_sentiment_score'] > row['lag1'] > row['lag2']:
        return '↗'
    if row['sector_sentiment_score'] < row['lag1'] < row['lag2']:
        return '↘'
    return '—'

sector_raw['momentum'] = sector_raw.apply(momentum_flag, axis=1)

# 6) Persist sector_sentiment_history with momentum
def persist_sector_history(df_sect):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sector_sentiment_history;"))
        conn.execute(text(
            "CREATE TABLE sector_sentiment_history ("
            "date DATE, sector TEXT, sector_sentiment_score DOUBLE PRECISION, momentum TEXT)"
        ))
        df_sect[['date','sector','sector_sentiment_score','momentum']].to_sql(
            'sector_sentiment_history', conn, if_exists='append', index=False
        )

persist_sector_history(sector_raw)
print(f"Computed {len(sector_raw)} rows into sector_sentiment_history (with momentum).")

# 7) Compute overall Pulse: equal-weighted average of 14 sectors per day
pulse_df = (
    sector_raw.groupby('date')['sector_sentiment_score']
      .mean()
      .reset_index(name='pulse_score')
)

# 8) Persist pulse_history
def persist_pulse_history(df_pulse):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS pulse_history;"))
        conn.execute(text(
            "CREATE TABLE pulse_history ("
            "date DATE, pulse_score DOUBLE PRECISION)"
        ))
        df_pulse.to_sql('pulse_history', conn, if_exists='append', index=False)

persist_pulse_history(pulse_df)
print(f"Rebuilt pulse_history with {len(pulse_df)} dates (equal-weight sectors).")
