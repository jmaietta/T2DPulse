# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild the full T2D Pulse history by market-cap weighting across sectors
# using data from Postgres (no CSVs involved).

#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(db_url)

# 2) Build sector-level market-cap history if not present (or refresh)
with engine.begin() as conn:
    # Drop old sector table and recreate
    conn.execute(text("DROP TABLE IF EXISTS sector_market_cap_history;"))
    conn.execute(text(
        "CREATE TABLE sector_market_cap_history "
        "(date DATE, sector TEXT, sector_cap DOUBLE PRECISION);"
    ))
# Aggregate from ticker-level data
df_mcap = pd.read_sql(
    "SELECT date, sector, SUM(market_cap) AS sector_cap "
    "FROM market_cap_history GROUP BY date, sector", engine
)
# Persist sector caps
df_mcap.to_sql("sector_market_cap_history", engine, if_exists="append", index=False)
print(f"Built sector_market_cap_history with {len(df_mcap)} rows.")

# 3) Load sector sentiment history
df_sent = pd.read_sql(
    "SELECT date, sector, sector_sentiment_score FROM sector_sentiment_history", engine
)
# 4) Load sector market-cap history
df_cap = pd.read_sql(
    "SELECT date, sector, sector_cap FROM sector_market_cap_history", engine
)

# 5) Normalize date types
df_sent['date'] = pd.to_datetime(df_sent['date']).dt.date
df_cap['date']  = pd.to_datetime(df_cap['date']).dt.date

# 6) Merge sentiment with market-cap
df = df_sent.merge(df_cap, on=["date","sector"])

# 7) Compute total sector_cap per date
total_cap = (
    df.groupby("date")["sector_cap"].sum()
      .reset_index(name="total_cap")
)
# 8) Merge total_cap back
df = df.merge(total_cap, on="date")

# 9) Calculate sector weight and weighted sentiment
df['weight'] = df['sector_cap'] / df['total_cap']
df['weighted_sent'] = df['sector_sentiment_score'] * df['weight']

# 10) Aggregate to get Pulse score per date
pulse_df = (
    df.groupby("date")["weighted_sent"]
      .sum()
      .reset_index(name="pulse_score")
)

# 11) Truncate and reload pulse_history table
with engine.begin() as conn:
    conn.execute(text("TRUNCATE pulse_history;"))
    conn.execute(
        text("INSERT INTO pulse_history(date, pulse_score) VALUES (:date, :pulse_score)"),
        pulse_df.to_dict(orient='records')
    )

print(f"Rebuilt pulse_history with {len(pulse_df)} dates (market-cap weighted across sectors).")
