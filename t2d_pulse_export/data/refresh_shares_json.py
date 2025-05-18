#!/usr/bin/env python3
"""
Fetch current *shares‑outstanding* for every ticker we track and
rewrite t2d_pulse_export/data/shares.json (values = millions of shares).

It tries APIs in this order:
  1. Finnhub  -> metric=company.sharesOutstanding   (needs FINNHUB_API_KEY)
  2. yfinance -> Ticker.info["sharesOutstanding"]   (no key needed)

Any ticker it can’t resolve will stop the script with a clear error
so a bad placeholder never sneaks in again.
"""

from __future__ import annotations
import json, os, pathlib, sys, time
from typing import Dict, List, Optional

import requests
import yfinance as yf


# ------------------------------------------------------------
# 1️⃣  Where to save the file
JSON_PATH = pathlib.Path("t2d_pulse_export/data/shares.json")

# 2️⃣  Where to get the tickers
#     Option A – derive from the EXISTING JSON (simplest):
if JSON_PATH.exists():
    TICKERS: List[str] = list(json.loads(JSON_PATH.read_text()).keys())
else:
    # Option B – hard‑code them here if you prefer:
    TICKERS = [
        "AAPL","ABNB","ACN","ADBE","ADSK","AFRM","AMAT","AMD","AMZN","APP",
        "APPS","ARM","AVGO","BABA","BILL","BKNG","CCCS","CHKP","CHWY","COIN",
        "CPRT","CRM","CRTO","CRWD","CSCO","CSGP","CTSH","CYBR","DDOG","DELL",
        "DV","DXC","EBAY","ESTC","ETSY","FI","FIS","FTNT","GOOGL","GPN","GTLB",
        "GWRE","HPQ","HUBS","IBM","ICE","INFY","INTC","INTU","LOGI","MDB",
        "META","MGNI","MSFT","NET","NFLX","NOW","NVDA","OKTA","ORCL","PANW",
        "PCOR","PDD","PINS","PLTR","PSTG","PUBM","PYPL","QCOM","S","SAP","SE",
        "SHOP","SMCI","SNAP","SNOW","SPOT","SSNC","SSYS","STX","TEAM","TRIP",
        "TSM","TTAN","TTD","WDAY","WDC","WIT","WMT","XYZ","YELP","ZS",
    ]

if not TICKERS:
    sys.exit("❌  No tickers found to process.")


# ------------------------------------------------------------
# 3️⃣  Helpers to fetch share counts
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")          # optional
FINNHUB_URL = "https://finnhub.io/api/v1/stock/metric"

def finnhub_shares(ticker: str) -> Optional[int]:
    if not FINNHUB_KEY:
        return None
    try:
        r = requests.get(
            FINNHUB_URL,
            params={"symbol": ticker, "metric": "company", "token": FINNHUB_KEY},
            timeout=10,
        )
        r.raise_for_status()
        so = r.json()["metric"].get("sharesOutstanding")
        return int(round(so / 1_000_000)) if so else None
    except Exception:
        return None

def yfinance_shares(ticker: str) -> Optional[int]:
    try:
        so = yf.Ticker(ticker).info.get("sharesOutstanding")
        return int(round(so / 1_000_000)) if so else None
    except Exception:
        return None


# ------------------------------------------------------------
# 4️⃣  Main loop
shares: Dict[str, int] = {}
missing: List[str] = []

for i, tkr in enumerate(TICKERS, 1):
    count = finnhub_shares(tkr) or yfinance_shares(tkr)
    if count:
        shares[tkr] = count
    else:
        missing.append(tkr)

    # keep free‑tier API limits happy
    time.sleep(0.25)

    if i % 20 == 0 or i == len(TICKERS):
        print(f"Fetched {i}/{len(TICKERS)} tickers…")


# ------------------------------------------------------------
# 5️⃣  Fail fast if anything is missing
if missing:
    sys.exit(f"❌  No share count returned for: {', '.join(missing)}")

# 6️⃣  Write out the cleaned JSON
JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
JSON_PATH.write_text(json.dumps(shares, indent=2))
print(f"✅  Wrote {len(shares)} tickers to {JSON_PATH}")
