#!/usr/bin/env python3
"""
Generate t2d_pulse_export/data/ticker_sector_map.json

* Reads tickers from shares.json             (already refreshed each build)
* Tries Finnhub first (company profile)      -> gicsSector / finnhubIndustry
* Falls back to yfinance                     -> info["sector"]
* Fails fast if any ticker is still missing
"""

from __future__ import annotations
import json, os, pathlib, sys, time
from typing import Dict, List, Optional

import requests, yfinance as yf

DATA_DIR = pathlib.Path(__file__).parent
SHARES_JSON = DATA_DIR / "shares.json"
OUTPUT      = DATA_DIR / "ticker_sector_map.json"

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")   # same key you already set

# ----------------------------------------------------------------------
tickers: List[str] = list(json.loads(SHARES_JSON.read_text()).keys())
if not tickers:
    sys.exit("❌  shares.json is empty – nothing to map")

def sector_finnhub(tkr: str) -> Optional[str]:
    if not FINNHUB_KEY:
        return None
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={"symbol": tkr, "token": FINNHUB_KEY},
            timeout=7,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("gicsSector") or data.get("finnhubIndustry")
    except Exception:
        return None

def sector_yf(tkr: str) -> Optional[str]:
    try:
        return yf.Ticker(tkr).info.get("sector")
    except Exception:
        return None

mapping: Dict[str, str] = {}
missing: List[str] = []

for i, t in enumerate(tickers, 1):
    sector = sector_finnhub(t) or sector_yf(t)
    if sector:
        mapping[t] = sector
    else:
        missing.append(t)
    time.sleep(0.15)          # keep within free‑tier rate limits

if missing:
    sys.exit(f"❌  No sector for: {', '.join(missing)}")

OUTPUT.write_text(json.dumps(mapping, indent=2))
print(f"✅  Wrote {len(mapping)} tickers → {OUTPUT}")
