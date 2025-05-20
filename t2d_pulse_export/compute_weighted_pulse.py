# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild the full T2D Pulse history by:
# 1) Aggregating stock-level sentiment (20-day EMA of returns) into sector-level sentiment via market-cap weighting
# 2) Computing the overall Pulse as the simple average of the 14 sector sentiment scores per day
# All data flows through Postgres—no CSVs.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL environment variable is not set")
engine = create_engine(db_url)

# 2) Load stock-level sentiment and market-cap history
stock_sent = pd.read_sql(
    "SELECT date, ticker, sentiment_score FROM stock_sentiment_history",
    engine
)
market_cap = pd.read_sql(
    "SELECT date, ticker, sector, market_cap FROM market_cap_history",
    engine
)
# Normalize date types\stock_sent['date'] = pd.to_datetime(stock_sent['date']).dt.date
market_cap['date']   = pd.to_datetime(market_cap['date']).dt.date

# 3) Merge to compute weighted contribution per stock
df = stock_sent.merge(market_cap, on=["date","ticker"])
# Compute total market cap per date but scoped to that sector group
total_cap_by_date = df.groupby(['date','sector'])['market_cap']
merged = df.copy()
merged['sector_total_cap'] = merged.groupby(['date','sector'])['market_cap'].transform('sum')
# Weight each stock's sentiment within its sector
merged['weight'] = merged['market_cap'] / merged['sector_total_cap']
merged['weighted_sent'] = merged['sentiment_score'] * merged['weight']

# 4) Aggregate to sector sentiment per date
sector_sent = (
    merged
    .groupby(['date','sector'])['weighted_sent']
    .sum()
    .reset_index(name='sector_sentiment_score')
)

# Persist sector_sentiment_history
df_sec = sector_sent.copy()
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS sector_sentiment_history;"))
    conn.execute(text(
        "CREATE TABLE sector_sentiment_history ("
        "date DATE, sector TEXT, sector_sentiment_score DOUBLE PRECISION)"
    ))
    df_sec.to_sql('sector_sentiment_history', conn, if_exists='append', index=False)
print(f"Computed {len(df_sec)} rows into sector_sentiment_history.")

# 5) Compute overall T2D Pulse as simple average of sector sentiment per date
pulse_df = (
    sector_sent
    .groupby('date')['sector_sentiment_score']
    .mean()
    .reset_index(name='pulse_score')
)

# 6) Persist pulse_history
with engine.begin() as conn:
    conn.execute(text("TRUNCATE pulse_history;"))
    conn.execute(
        text("INSERT INTO pulse_history(date, pulse_score) VALUES (:date, :pulse_score)"),
        pulse_df.to_dict(orient='records')
    )

print(f"Rebuilt pulse_history with {len(pulse_df)} dates (equal-weighted across sectors).")
