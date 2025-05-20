# compute_stock_sentiment.py
# -------------------------------------------
# Derive stock-level sentiment_score as the 20-day EMA of daily market-cap returns,
# using market_cap_history from Postgres (no CSVs), and load into stock_sentiment_history.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Load market-cap history as proxy for price
#    Table: market_cap_history(date DATE, ticker TEXT, sector TEXT, market_cap DOUBLE PRECISION)
df = pd.read_sql(
    "SELECT date, ticker, market_cap FROM market_cap_history ORDER BY ticker, date",
    engine
)

# 3) Normalize and sort
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['ticker','date'])

# 4) Compute daily returns per ticker
#    daily_return = (mcap_t / mcap_{t-1}) - 1
df['daily_return'] = df.groupby('ticker')['market_cap'].pct_change()

# 5) Calculate 20-day EMA of returns as sentiment proxy
sentiment = (
    df
    .groupby('ticker')['daily_return']
    .apply(lambda x: x.ewm(span=20, adjust=False).mean())
    .reset_index(name='sentiment_score')
)

# 6) Merge sentiment back with dates
df_sent = df[['date','ticker']].reset_index(drop=True)
df_sent = df_sent.merge(
    sentiment,
    left_on=['ticker', 'date'],
    right_on=['ticker', 'date'],
    how='left'
)

# 7) Prepare final DataFrame: one row per date,ticker
df_final = df_sent[['date','ticker','sentiment_score']].dropna()
#    Convert to date only
df_final['date'] = df_final['date'].dt.date

# 8) Persist into stock_sentiment_history
table_sql = (
    "CREATE TABLE IF NOT EXISTS stock_sentiment_history ("
    "date DATE, ticker TEXT, sentiment_score DOUBLE PRECISION)"
)
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS stock_sentiment_history;"))
    conn.execute(text(table_sql))
    df_final.to_sql('stock_sentiment_history', conn, if_exists='append', index=False)

print(f"Generated and loaded {len(df_final)} rows into stock_sentiment_history.")
