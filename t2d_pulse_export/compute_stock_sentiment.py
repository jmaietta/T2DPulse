# compute_stock_sentiment.py
# -------------------------------------------
# Compute full historical stock-level sentiment as the 3-day EMA of daily market-cap returns,
# normalize to a 0–100 scale, dedupe one row per ticker-date, and populate stock_sentiment_history table.
# Then display the most recent 20 trading-day history of the 3-day EMA sentiment for GOOGL.

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Load deduplicated market-cap history as price proxy
#    Use MAX(market_cap) per ticker-date to collapse duplicates across sectors
df = pd.read_sql(
    """
    SELECT date, ticker, MAX(market_cap) AS market_cap
      FROM market_cap_history
     GROUP BY date, ticker
     ORDER BY ticker, date
    """,
    engine
)

# 3) Convert to datetime and sort
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['ticker', 'date']).reset_index(drop=True)

# 4) Compute daily returns per ticker
df['daily_return'] = df.groupby('ticker')['market_cap'].pct_change()

# 5) Compute raw 3-day EMA of returns for sentiment proxy
#    α = 2/(span+1) with span=3
df['raw_sentiment_score'] = (
    df.groupby('ticker')['daily_return']
      .transform(lambda x: x.ewm(span=3, adjust=False).mean())
)

# 6) Normalize to 0–100 scale: percentage ×100 then +50 shift
df['sentiment_score'] = df['raw_sentiment_score'] * 100 + 50

# 7) Prepare final DataFrame: drop NaN, convert date to date-only, and dedupe
df_final = df[['date','ticker','sentiment_score','raw_sentiment_score']].dropna().copy()
df_final['date'] = df_final['date'].dt.date  # convert datetime to date
# ensure one entry per ticker-date
df_final = df_final.drop_duplicates(subset=['date','ticker'])

# 8) Persist into stock_sentiment_history table
with engine.begin() as conn:
    # Recreate the table\    
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

# 9) Display the most recent 20 trading-day history of 3-day EMA sentiment for GOOGL
try:
    googl = df_final[df_final['ticker'] == 'GOOGL']
    googl = googl.sort_values('date', ascending=False).head(20)
    print("\nLast 20 Trading-Day Sentiment History (3-day EMA) for GOOGL:")
    print(googl[['date','sentiment_score']].to_string(index=False))
except Exception as e:
    print(f"Could not display GOOGL history: {e}")
