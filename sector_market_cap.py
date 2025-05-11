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
import time
import json
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

try:
    import finnhub
except ImportError:
    finnhub = None  # Finnhub optional – yfinance works without it

try:
    import nasdaqdatalink
except ImportError:
    nasdaqdatalink = None  # NASDAQ Data Link optional

# For more reliable market cap data
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
NASDAQ_API_KEY = os.environ.get("NASDAQ_DATA_LINK_API_KEY")

################################################################################
# 🗄️  CONFIG –‑ EDIT YOUR SECTOR LISTS HERE (no other file touch‑points)
################################################################################
SECTORS: Dict[str, List[str]] = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
    "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC", "XYZ"],
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

def _polygon_market_cap(ticker: str) -> Optional[float]:
    """Source 1 (Primary) – Polygon.io API for most reliable market cap data."""
    if not POLYGON_API_KEY:
        return None
    
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Polygon API error for {ticker}: {response.status_code}")
            return None
            
        data = response.json()
        
        if data.get("results"):
            # Get market cap from Polygon data
            market_cap = data["results"].get("market_cap")
            if market_cap:
                return float(market_cap)
            
            # If no direct market cap, try calculating from shares * price
            shares = data["results"].get("share_class_shares_outstanding")
            price = data["results"].get("price")
            if not price:
                # If price not in ticker details, try quotes endpoint
                quote_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
                quote_response = requests.get(quote_url, timeout=10)
                if quote_response.status_code == 200:
                    quote_data = quote_response.json()
                    if quote_data.get("ticker") and quote_data.get("ticker", {}).get("day"):
                        price = quote_data["ticker"]["day"].get("c")  # close price
                        
            if shares and price:
                return float(shares) * float(price)
                
        return None
    except Exception as e:
        print(f"Polygon API error for {ticker}: {e}")
        return None


def _nasdaq_market_cap(ticker: str) -> Optional[float]:
    """Source 2 – NASDAQ Data Link API for fundamental data."""
    if not NASDAQ_API_KEY:
        return None
    
    try:
        headers = {
            "X-API-KEY": NASDAQ_API_KEY,
            "Accept": "application/json"
        }
        # Try Sharadar Core US Fundamentals (SF1) for market cap
        url = f"https://data.nasdaq.com/api/v3/datatables/SHARADAR/SF1?ticker={ticker}&dimension=MRQ&api_key={NASDAQ_API_KEY}"
        
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"NASDAQ API error for {ticker}: {response.status_code}")
            return None
            
        data = response.json()
        
        # Check if we have valid data
        if data.get("datatable") and data["datatable"].get("data") and len(data["datatable"]["data"]) > 0:
            # Find the marketcap column index
            columns = data["datatable"]["columns"]
            marketcap_idx = None
            
            for i, col in enumerate(columns):
                if col.get("name") == "marketcap":
                    marketcap_idx = i
                    break
                    
            if marketcap_idx is not None and len(data["datatable"]["data"][0]) > marketcap_idx:
                marketcap = data["datatable"]["data"][0][marketcap_idx]
                if marketcap:
                    return float(marketcap) * 1_000_000  # Convert to raw dollars
        
        return None
    except Exception as e:
        print(f"NASDAQ API error for {ticker}: {e}")
        return None


def _yf_market_cap(ticker: str) -> Optional[float]:
    """Source 3 – yfinance fast_info then fallback to infodict."""
    try:
        tk = yf.Ticker(ticker)
        mc = tk.fast_info.get("market_cap")
        if mc is None:
            mc = tk.info.get("marketCap")
        return float(mc) if mc else None
    except Exception as e:
        print(f"YFinance error for {ticker}: {e}")
        return None


def _fh_market_cap(ticker: str, fh_client) -> Optional[float]:
    """Source 4 – Finnhub metric endpoint."""
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
    except Exception as e:
        print(f"Finnhub error for {ticker}: {e}")
        return None
    return None


def fetch_market_cap(ticker: str, fh_client) -> Optional[float]:
    """Return market cap using cascading data sources, prioritizing most reliable.
    
    Data sources in order of preference:
    1. Polygon.io API (most reliable)
    2. NASDAQ Data Link API
    3. yfinance API
    4. Finnhub API
    
    Returns None only if all sources fail.
    """
    # Try each source in order, logging each attempt
    print(f"Fetching market cap for {ticker}...")
    
    # 1. Polygon API (most reliable)
    mc = _polygon_market_cap(ticker)
    if mc is not None:
        print(f"  ✓ Polygon: ${mc/1_000_000_000:.2f}B")
        return mc
    print(f"  ✗ Polygon: Failed")
    
    # 2. NASDAQ Data Link
    mc = _nasdaq_market_cap(ticker)
    if mc is not None:
        print(f"  ✓ NASDAQ: ${mc/1_000_000_000:.2f}B")
        return mc
    print(f"  ✗ NASDAQ: Failed")
    
    # 3. YFinance
    mc = _yf_market_cap(ticker)
    if mc is not None:
        print(f"  ✓ YFinance: ${mc/1_000_000_000:.2f}B")
        return mc
    print(f"  ✗ YFinance: Failed")
    
    # 4. Finnhub (last resort)
    mc = _fh_market_cap(ticker, fh_client)
    if mc is not None:
        print(f"  ✓ Finnhub: ${mc/1_000_000_000:.2f}B")
        return mc
    print(f"  ✗ Finnhub: Failed")
    
    # All sources failed
    print(f"  ✗ All sources failed for {ticker}")
    return None


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
        # Open in append mode to add to the log
        with open(MISSING_LOG, 'a') as f:
            f.write(f"{today}: {', '.join(missing_any)}\n")
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
    """Return dictionary of latest market caps for each sector (for app.py integration).
    
    This function has been updated to use the PostgreSQL database instead of CSV files.
    It connects to the PostgreSQL database using the DATABASE_URL environment variable.
    """
    import psycopg2
    import os
    from datetime import date, timedelta
    
    # Try to get data from PostgreSQL database first
    try:
        database_url = os.environ.get('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            # Get the latest date with data
            cursor.execute("SELECT MAX(date) FROM sector_market_caps")
            latest_date = cursor.fetchone()[0]
            
            if latest_date:
                # Get sector market caps for the latest date
                cursor.execute("""
                    SELECT s.name, smc.market_cap 
                    FROM sector_market_caps smc
                    JOIN sectors s ON smc.sector_id = s.id
                    WHERE smc.date = %s
                """, (latest_date,))
                
                results = cursor.fetchall()
                conn.close()
                
                if results:
                    print(f"Using database data from {latest_date}")
                    return {sector: market_cap for sector, market_cap in results}
    except Exception as e:
        print(f"Error getting data from database: {e}")
        # Fall back to CSV if database access fails
    
    # Fall back to CSV if database is empty or not available
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