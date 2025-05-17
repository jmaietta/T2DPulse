"""
fetch_shares.py
--------------------------------------------------
One‑time helper that pulls each ticker’s fully‑diluted
shares outstanding from Polygon and saves them to
data/shares.json .

Prerequisites:
  • POLYGON_API_KEY set as an environment variable
  • T2DPulse_ticker_sector_mapping.txt already present
Run this from the t2d_pulse_export folder:
  python fetch_shares.py
"""

import os, json, pandas as pd
from polygon import RESTClient

API_KEY   = os.getenv("POLYGON_API_KEY")
MAP_FILE  = "T2DPulse_ticker_sector_mapping.txt"
OUT_PATH  = "data/shares.json"

assert API_KEY, "POLYGON_API_KEY env‑var missing!"

# 1) read mapping → list of tickers
mapping = pd.read_csv(MAP_FILE)
tickers = sorted(mapping["Ticker"].unique())

# 2) query Polygon once per symbol
client  = RESTClient(API_KEY)
shares  = {}

for tkr in tickers:
    try:
        ref = client.reference_ticker(tkr)
        shares[tkr] = (
            ref.nextSharesOutstanding
            or ref.sharesOutstanding
            or ref.weighted_shares_outstanding or 0
        )
        print(f"{tkr:<6}  {shares[tkr]:,.0f}")
    except Exception as e:
        print(f"⚠️  {tkr}: {e}")

# 3) save
os.makedirs("data", exist_ok=True)
with open(OUT_PATH, "w") as f:
    json.dump(shares, f, indent=2)

print(f"\n✔  Wrote {len(shares)} records to {OUT_PATH}")
