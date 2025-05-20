# compute_stock_sentiment.py
# -------------------------------------------
# Derive a stock-level sentiment_score as the 20-day EMA of daily returns
# using Postgres-only data (no CSVs involved), and load into stock_sentiment_history.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(DATABASE_URL)

# 2) Load historical close prices
#    Assumes you have a table `stock_prices(date DATE, ticker TEXT, close DOUBLE PRECISION)`
df = pd.read_sql(
    "SELECT date, ticker, close FROM stock_prices ORDER BY ticker, date",
    engine
)
# Normalize and sort
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['ticker','date'])

# 3) Compute daily returns per ticker
#    daily_return = (today_close / yesterday_close) - 1
df['daily_return'] = df.groupby('ticker')['close'].pct_change()

# 4) Compute 20-day exponential moving average (EMA) of returns
#    This serves as our stock-level sentiment proxy
#    span=20 uses the same smoothing as a 20-day EMA
sent = (
    df
    .groupby('ticker')['daily_return']
    .apply(lambda x: x.ewm(span=20, adjust=False).mean())
    .reset_index(name='sentiment_score')
)
# Merge sentiment_score back to date and ticker
df_sent = df[['date','ticker']].merge(sent, on=['ticker', df.index.name], how='left')

# 5) Prepare final DataFrame: one row per date,ticker
df_final = df_sent[['date','ticker','sentiment_score']].dropna()
# Convert date to date (drop time)
df_final['date'] = df_final['date'].dt.date

# 6) Persist into `stock_sentiment_history`
with engine.begin() as conn:
    # Recreate the table\    
    conn.execute(text("DROP TABLE IF EXISTS stock_sentiment_history;"))
    conn.execute(text(
        "CREATE TABLE stock_sentiment_history ("
        "date DATE, ticker TEXT, sentiment_score DOUBLE PRECISION)"
    ))
    # Bulk insert
    df_final.to_sql("stock_sentiment_history", conn, if_exists='append', index=False)

print(f"Generated and loaded {len(df_final)} rows into stock_sentiment_history.")
