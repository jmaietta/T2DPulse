"""
sector_market_cap.py  ░░  T2D Pulse – Daily Sector‑Level Market‑Cap Engine
────────────────────────────────────────────────────────────────────────────
This single file replaces the brittle web of helper modules that grew inside
Replit.  Run it once per trading day (e.g. via Replit's *Scheduler*).  It will

1.  Pull the latest market‑cap for every ticker in each tech‑sector list.
2.  Sum them to a sector total and append the figures to
    `sector_market_caps.csv` (persistent storage).
3.  Regenerate an interactive Plotly HTML chart (`sector_caps_chart.html`).

Key design choices
──────────────────
* **Two independent data sources** – yfinance (no key) first, Finnhub second
  (requires *FINNHUB_API_KEY* in Replit Secrets).  If both fail the ticker is
  reported but *never* filled with synthetic data.
* **Single CSV** – keeps an additive, version‑controlled history that your Dash
  front‑end can query directly.
* **Graceful logging** – any missing tickers are written to
  `missing_tickers.log` so you can decide whether to replace or retire them.
* **Zero side‑effects** – the script touches only the three artefacts above and
  does not import any of the legacy helper files.

────────────────────────────────────────────────────────────────────────────
DEPLOYMENT STEPS (one‑time)
───────────────────────────
$ pip install --upgrade yfinance finnhub-python pandas plotly
# Add FINNHUB_API_KEY in *Secrets* → key: FINNHUB_API_KEY / value: your_key
# Activate Replit Scheduler: 16:00 America/New_York, command → python sector_market_cap.py

Dash integration (example snippet)
──────────────────────────────────
from sector_market_cap import chart_sector_caps
chart_path = chart_sector_caps()  # returns 'sector_caps_chart.html'
with open(chart_path) as f:
    dash_html_components.Iframe(srcDoc=f.read(), style={"width":"100%","height":"500px"})
"""
import os
import sys
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

try:
    import finnhub
except ImportError:
    finnhub = None  # Finnhub optional – yfinance works without it

################################################################################
# 🗄️  CONFIG –‑ EDIT YOUR SECTOR LISTS HERE (no other file touch‑points)
################################################################################
SECTORS: Dict[str, List[str]] = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
    "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],
    "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
    "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
    "IT Services / Legacy Tech": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
    "Hardware / Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
    "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
    "Dev Tools / Analytics": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
    "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
    "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
    "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
    "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
    "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
}
################################################################################
CSV_PATH = Path("sector_market_caps.csv")
CHART_PATH = Path("sector_caps_chart.html")
MISSING_LOG = Path("missing_tickers.log")
################################################################################

def _yf_market_cap(ticker: str) -> Optional[float]:
    """Primary source – yfinance fast_info then fallback to infodict."""
    try:
        tk = yf.Ticker(ticker)
        mc = tk.fast_info.get("market_cap")
        if mc is None:
            mc = tk.info.get("marketCap")
        return float(mc) if mc else None
    except Exception:
        return None


def _fh_market_cap(ticker: str, fh_client) -> Optional[float]:
    """Secondary source – Finnhub metric endpoint."""
    if fh_client is None:
        return None
    try:
        metric = fh_client.stock_metric(symbol=ticker, metric="all")
        mc = metric.get("metric", {}).get("marketCapitalization")
        if mc:
            return float(mc) * 1_000_000  # Finnhub returns $MM
        # Try price * shares outstanding
        quote = fh_client.quote(ticker)
        price = quote.get("c")
        shares = metric.get("metric", {}).get("sharesOutstanding")
        if price and shares:
            return float(price) * float(shares)
    except Exception:
        return None
    return None


def fetch_market_cap(ticker: str, fh_client) -> Optional[float]:
    """Return market cap using yfinance → Finnhub cascade.  None if both fail."""
    mc = _yf_market_cap(ticker)
    if mc is None:
        mc = _fh_market_cap(ticker, fh_client)
    return mc


def calculate_sector_caps(sectors: Dict[str, List[str]]) -> pd.DataFrame:
    """Return DataFrame with today's sector market‑cap totals & missing tickers."""
    api_key = os.getenv("FINNHUB_API_KEY")
    fh_client = finnhub.Client(api_key=api_key) if finnhub and api_key else None

    today = dt.date.today().isoformat()
    records: List[Tuple[str, str, float, str]] = []
    missing_any = []

    for sector, tickers in sectors.items():
        total_cap = 0.0
        missing = []
        for tkr in tickers:
            cap = fetch_market_cap(tkr, fh_client)
            if cap is None:
                missing.append(tkr)
            else:
                total_cap += cap
        records.append((today, sector, total_cap, ";".join(missing)))
        if missing:
            missing_any.extend(missing)

    if missing_any:
        MISSING_LOG.write_text(f"{today}: {', '.join(missing_any)}\n", append=True)
    return pd.DataFrame(records, columns=["date", "sector", "market_cap", "missing_tickers"])


def append_to_csv(df: pd.DataFrame, path: Path = CSV_PATH) -> pd.DataFrame:
    """Merge today's data with existing history (dedupe on date+sector)."""
    if path.exists():
        hist = pd.read_csv(path)
        combined = pd.concat([hist, df]).drop_duplicates(subset=["date", "sector"], keep="last")
    else:
        combined = df
    combined.to_csv(path, index=False)
    return combined


def chart_sector_caps(path: Path = CSV_PATH, out: Path = CHART_PATH) -> Path:
    """Build interactive Plotly line chart and return its path."""
    import plotly.express as px  # Local import to keep base import light
    df = pd.read_csv(path)
    if df.empty:
        print("[WARN] No data to chart yet.")
        return out
    fig = px.line(df, x="date", y="market_cap", color="sector",
                  title="T2D Pulse – Sector Market Caps Over Time",
                  labels={"market_cap": "Market Cap (USD)", "date": "Date"})
    fig.update_layout(legend_title_text="Sector", hovermode="x unified")
    fig.write_html(out, include_plotlyjs="cdn", full_html=True)
    return out


def get_latest_sector_caps() -> Dict[str, float]:
    """Return dictionary of latest market caps for each sector (for app.py integration)."""
    if not CSV_PATH.exists():
        return {}
    
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        return {}
    
    # Get the latest date in the dataset
    latest_date = df['date'].max()
    
    # Filter to just that date and create a sector -> market_cap dictionary
    latest_df = df[df['date'] == latest_date]
    return dict(zip(latest_df['sector'], latest_df['market_cap']))


def format_sector_caps_for_display(billions=True):
    """Format latest sector caps for display in the dashboard."""
    caps = get_latest_sector_caps()
    if not caps:
        return pd.DataFrame()
    
    # Convert to dataframe and sort by market cap descending
    df = pd.DataFrame({
        'Sector': list(caps.keys()),
        'Market Cap': list(caps.values())
    })
    
    # Convert to billions if requested
    if billions:
        df['Market Cap (Billions USD)'] = df['Market Cap'] / 1_000_000_000
        df = df.drop('Market Cap', axis=1)
    
    return df.sort_values('Market Cap (Billions USD)', ascending=False).reset_index(drop=True)


def main():
    print("▶︎ Calculating daily sector market caps …")
    today_df = calculate_sector_caps(SECTORS)
    hist_df = append_to_csv(today_df)
    chart_path = chart_sector_caps()
    
    # Create a formatted display of the latest data
    latest_df = format_sector_caps_for_display()
    if not latest_df.empty:
        print("\nLatest Sector Market Caps (Billions USD):")
        print("-" * 60)
        for _, row in latest_df.iterrows():
            print(f"{row['Sector']:<25} ${row['Market Cap (Billions USD)']:.2f}B")
    
    print(f"\n✓ Updated {CSV_PATH}  |  ✓ Wrote chart → {chart_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)