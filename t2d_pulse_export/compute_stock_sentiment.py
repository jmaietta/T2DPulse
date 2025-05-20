# compute_stock_sentiment.py
# -------------------------------------------
# Incrementally update stock-level sentiment_score as the 20-day EMA of daily market-cap returns,
# preserving continuity between trading days, normalizing to a 0–100 scale,
# and appending only the new day's data to stock_sentiment_history.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Fetch the previous day's raw_sentiment_score for each ticker
prev_df = pd.read_sql(
    """
    SELECT ticker, raw_sentiment_score
      FROM stock_sentiment_history
     WHERE date = (
         SELECT MAX(date) FROM stock_sentiment_history
     )
    """,
    engine
)
prev_raw = dict(zip(prev_df['ticker'], prev_df['raw_sentiment_score']))

# 3) Get the latest market-cap and prior-day market-cap for each ticker
mcap_df = pd.read_sql(
    """
    WITH mc AS (
      SELECT date, ticker, market_cap,
             LAG(market_cap) OVER (PARTITION BY ticker ORDER BY date) AS prev_mcap
        FROM market_cap_history
    ), latest AS (
      SELECT * FROM mc
       WHERE date = (SELECT MAX(date) FROM market_cap_history)
    )
    SELECT date, ticker, market_cap, prev_mcap
      FROM latest
     WHERE prev_mcap IS NOT NULL
    """,
    engine
)
# ensure date type
mcap_df['date'] = pd.to_datetime(mcap_df['date']).dt.date

# 4) Compute daily return
mcap_df['daily_return'] = mcap_df['market_cap'] / mcap_df['prev_mcap'] - 1

# 5) Update raw sentiment with 20-day EMA formula: EMA_new = α*R_new + (1–α)*EMA_prev
alpha = 2 / (20 + 1)

def compute_raw_ema(row):
    prev = prev_raw.get(row['ticker'], 0.0)
    return alpha * row['daily_return'] + (1 - alpha) * prev

mcap_df['raw_sentiment_score'] = mcap_df.apply(compute_raw_ema, axis=1)

# 6) Normalize to 0–100: percent form + 50 shift
mcap_df['sentiment_score'] = mcap_df['raw_sentiment_score'] * 100 + 50

# 7) Persist only the new day's sentiment into stock_sentiment_history
with engine.begin() as conn:
    # Ensure table exists with raw_sentiment_score column
    conn.execute(text(
        "CREATE TABLE IF NOT EXISTS stock_sentiment_history ("
        "date DATE, ticker TEXT, sentiment_score DOUBLE PRECISION, raw_sentiment_score DOUBLE PRECISION)"
    ))
    # Insert today's records
    insert_sql = text(
        "INSERT INTO stock_sentiment_history(date, ticker, sentiment_score, raw_sentiment_score)"
        " VALUES (:date, :ticker, :sentiment_score, :raw_sentiment_score)"
    )
    conn.execute(insert_sql, mcap_df.to_dict(orient='records'))

print(f"Appended {len(mcap_df)} rows to stock_sentiment_history (incremental update).")
