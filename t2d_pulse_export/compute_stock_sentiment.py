# compute_stock_sentiment.py
# -------------------------------------------
# Compute full historical stock-level sentiment as the 20-day EMA of daily market-cap returns,
# normalize to 0–100 scale, and populate stock_sentiment_history table end-to-end.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Load full market-cap history as price proxy
#    Table: market_cap_history(date DATE, ticker TEXT, sector TEXT, market_cap DOUBLE PRECISION)
df = pd.read_sql(
    "SELECT date, ticker, market_cap FROM market_cap_history ORDER BY ticker, date",
    engine
)
# Convert to datetime and sort
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['ticker', 'date']).reset_index(drop=True)

# 3) Compute daily returns per ticker
df['daily_return'] = df.groupby('ticker')['market_cap'].pct_change()

# 4) Compute raw 20-day EMA of returns as sentiment proxy using transform
alpha = 2 / (20 + 1)
def calc_ema(x):
    return x.ewm(span=20, adjust=False).mean()
df['raw_sentiment_score'] = df.groupby('ticker')['daily_return'].transform(calc_ema)

# 5) Normalize to 0–100 scale: percentage plus 50 shift
df['sentiment_score'] = df['raw_sentiment_score'] * 100 + 50

# 6) Prepare final DataFrame and drop NaN entries
df_final = df[['date','ticker','sentiment_score','raw_sentiment_score']].dropna().copy()
# Convert date to date only
df_final['date'] = df_final['date'].dt.date

# 7) Persist into stock_sentiment_history end-to-end
with engine.begin() as conn:
    # Recreate the table
    conn.execute(text("DROP TABLE IF EXISTS stock_sentiment_history;"))
    conn.execute(text(
        "CREATE TABLE stock_sentiment_history ("
        "date DATE, ticker TEXT, sentiment_score DOUBLE PRECISION, raw_sentiment_score DOUBLE PRECISION)"
    ))
    # Bulk insert
    insert_sql = text(
        "INSERT INTO stock_sentiment_history(date, ticker, sentiment_score, raw_sentiment_score) "
        "VALUES(:date, :ticker, :sentiment_score, :raw_sentiment_score)"
    )
    conn.execute(insert_sql, df_final.to_dict(orient='records'))

print(f"Computed {len(df_final)} rows into stock_sentiment_history.")
