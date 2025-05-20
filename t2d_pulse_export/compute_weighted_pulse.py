import os
import pandas as pd
from sqlalchemy import create_engine, text

# compute_weighted_pulse.py
# -------------------------------------------
# Rebuild the full T2D Pulse history by market-cap weighting
# across sectors, using Postgres-only data (no CSVs).

# 1) Connect to Postgres
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(DATABASE_URL)

# 2) Load sector sentiment and sector market-cap history
sent_sql = "SELECT date, sector, sector_sentiment_score FROM sector_sentiment_history"
cap_sql  = "SELECT date, sector, sector_cap FROM sector_market_cap_history"

df_sent = pd.read_sql(sent_sql, engine)
df_cap  = pd.read_sql(cap_sql, engine)

# 3) Merge on date and sector
df = df_sent.merge(df_cap, on=["date", "sector"] )

# 4) Compute total market cap per date
df["date"] = pd.to_datetime(df["date"]).dt.date
total_cap = df.groupby("date")["sector_cap"].sum().reset_index(name="total_cap")

# 5) Join total_cap back to df
merged = df.merge(total_cap, on="date")
merged["weight"] = merged["sector_cap"] / merged["total_cap"]

# 6) Compute weighted pulse score per date
merged["weighted_sent"] = merged["sector_sentiment_score"] * merged["weight"]
pulse_df = (
    merged
    .groupby("date")["weighted_sent"]
    .sum()
    .reset_index(name="pulse_score")
)

# 7) Truncate and load pulse_history table
with engine.begin() as conn:
    conn.execute(text("TRUNCATE pulse_history;"))
    conn.execute(
        text("INSERT INTO pulse_history(date, pulse_score) VALUES (:date, :pulse_score)"),
        pulse_df.to_dict(orient="records")
    )

print(f"Rebuilt pulse_history with {len(pulse_df)} dates (market-cap weighted across sectors).")
