# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild the full T2D Pulse history by market-cap weighting across sectors
# using data from Postgres (no CSVs involved).

import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1) Connect to Postgres
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(DATABASE_URL)

# 2) Load sector sentiment history
df_sent = pd.read_sql(
    "SELECT date, sector, sector_sentiment_score FROM sector_sentiment_history",
    engine
)
# 3) Load sector market-cap history
df_cap = pd.read_sql(
    "SELECT date, sector, sector_cap FROM sector_market_cap_history",
    engine
)

# 4) Normalize date types
df_sent['date'] = pd.to_datetime(df_sent['date']).dt.date
df_cap['date']  = pd.to_datetime(df_cap['date']).dt.date

# 5) Merge sentiment with market-cap
df = df_sent.merge(df_cap, on=["date","sector"])

# 6) Compute total sector_cap per date
total_cap = (
    df.groupby("date")
      ["sector_cap"].sum()
      .reset_index(name="total_cap")
)

# 7) Merge total_cap back
df = df.merge(total_cap, on="date")

# 8) Calculate sector weight and weighted sentiment
df['weight'] = df['sector_cap'] / df['total_cap']
df['weighted_sent'] = df['sector_sentiment_score'] * df['weight']

# 9) Aggregate to get Pulse score per date
pulse_df = (
    df
    .groupby("date")["weighted_sent"]
    .sum()
    .reset_index(name="pulse_score")
)

# 10) Truncate and reload pulse_history table
with engine.begin() as conn:
    conn.execute(text("TRUNCATE pulse_history;"))
    conn.execute(
        text("INSERT INTO pulse_history(date, pulse_score) VALUES (:date, :pulse_score)"),
        pulse_df.to_dict(orient='records')
    )

print(f"Rebuilt pulse_history with {len(pulse_df)} dates (market-cap weighted across sectors).")
