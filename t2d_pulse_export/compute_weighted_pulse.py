# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild sector and overall T2D Pulse history using a 3-day EMA of stock returns
# and sector sentiment, normalized 0–100 scale throughout.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Load stock-level sentiment (3-day EMA) and market-cap history
stock_sent = pd.read_sql(
    "SELECT date, ticker, sentiment_score AS stock_sentiment FROM stock_sentiment_history",
    engine
)
mcap = pd.read_sql(
    "SELECT date, ticker, sector, market_cap FROM market_cap_history",
    engine
)

# Normalize date types
stock_sent['date'] = pd.to_datetime(stock_sent['date']).dt.date
mcap['date'] = pd.to_datetime(mcap['date']).dt.date

# 3) Merge to get weighted contributions per stock
df = stock_sent.merge(mcap, on=["date","ticker"])

# Compute sector total cap per day
df['sector_total_cap'] = df.groupby(['date','sector'])['market_cap'].transform('sum')
# Weight each stock's sentiment within its sector
df['weight'] = df['market_cap'] / df['sector_total_cap']
df['weighted_sent'] = df['stock_sentiment'] * df['weight']

# 4) Aggregate to daily sector sentiment
sector_df = (
    df.groupby(['date','sector'])['weighted_sent']
      .sum()
      .reset_index(name='sector_sentiment_raw')
)

# 5) Smooth sector sentiment with a 3-day EMA
sector_df['sector_sentiment_score'] = (
    sector_df.groupby('sector')['sector_sentiment_raw']
      .transform(lambda x: x.ewm(span=3, adjust=False).mean())
)

# 6) Normalize sector sentiment to 0–100 (if desired) -- already on 0–100 scale via stock_sent
#    Here we assume sector_sentiment_raw was on 0–100, so EMA stays in same range

# 7) Persist sector_sentiment_history
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS sector_sentiment_history;"))
    conn.execute(text(
        "CREATE TABLE sector_sentiment_history ("
        "date DATE, sector TEXT, sector_sentiment_score DOUBLE PRECISION)"
    ))
    sector_df[['date','sector','sector_sentiment_score']].to_sql(
        'sector_sentiment_history', conn, if_exists='append', index=False
    )
print(f"Computed {len(sector_df)} rows into sector_sentiment_history.")

# 8) Compute overall Pulse: simple average of 14 sector scores per day
pulse_df = (
    sector_df.groupby('date')['sector_sentiment_score']
      .mean()
      .reset_index(name='pulse_score_raw')
)
# 9) Smooth Pulse with 3-day EMA
pulse_df['pulse_score'] = pulse_df['pulse_score_raw'].ewm(span=3, adjust=False).mean()

# 10) Persist pulse_history
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS pulse_history;"))
    conn.execute(text(
        "CREATE TABLE pulse_history ("
        "date DATE, pulse_score DOUBLE PRECISION)"
    ))
    pulse_df[['date','pulse_score']].to_sql(
        'pulse_history', conn, if_exists='append', index=False
    )
print(f"Rebuilt pulse_history with {len(pulse_df)} dates (3-day EMA across sectors).")
