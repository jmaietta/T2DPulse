# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild sector and overall T2D Pulse history using a 3-day EMA for sector sentiment
# calculated via equal-weighted average of stock sentiments, and then compute the Pulse as the
# simple daily average of the 14 sector scores (equal-weight sectors).
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

# 3) Load sector classification per ticker
df_sector = pd.read_sql(
    "SELECT DISTINCT ticker, sector FROM market_cap_history",
    engine
)

# 4) Merge stock sentiment with sector assignments
df = df_stock.merge(df_sector, on='ticker')

# 5) Compute raw sector sentiment: equal-weighted average of stock_sentiment per sector per day
sector_raw = (
    df.groupby(['date', 'sector'])['stock_sentiment']
      .mean()
      .reset_index(name='sector_sentiment_raw')
)

# 6) Smooth sector sentiment with a 3-day EMA
sector_raw['sector_sentiment_score'] = (
    sector_raw.groupby('sector')['sector_sentiment_raw']
      .transform(lambda x: x.ewm(span=3, adjust=False).mean())
)

# 7) Persist sector_sentiment_history
def persist_sector_history(df_sect):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sector_sentiment_history;"))
        conn.execute(text(
            "CREATE TABLE sector_sentiment_history ("
            "date DATE, sector TEXT, sector_sentiment_score DOUBLE PRECISION)"
        ))
        df_sect[['date','sector','sector_sentiment_score']].to_sql(
            'sector_sentiment_history', conn, if_exists='append', index=False
        )

persist_sector_history(sector_raw)
print(f"Computed {len(sector_raw)} rows into sector_sentiment_history.")

# 8) Compute overall Pulse: equal-weighted average of 14 sectors per day
pulse_df = (
    sector_raw.groupby('date')['sector_sentiment_score']
      .mean()
      .reset_index(name='pulse_score')
)

# 9) Persist pulse_history
def persist_pulse_history(df_pulse):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS pulse_history;"))
        conn.execute(text(
            "CREATE TABLE pulse_history ("
            "date DATE, pulse_score DOUBLE PRECISION)"
        ))
        df_pulse.to_sql(
            'pulse_history', conn, if_exists='append', index=False
        )

persist_pulse_history(pulse_df)
print(f"Rebuilt pulse_history with {len(pulse_df)} dates (equal-weight sectors).")
