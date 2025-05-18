# refresh_market_caps.py
# ETL script: fetch historical and current market caps and store in Postgres

import os
import datetime as dt

import pandas as pd
import yfinance as yf
import finnhub
import sqlalchemy

# --- Initialize Finnhub client for share counts ---
FINN = finnhub.Client(api_key=os.getenv("FINNHUB_KEY"))

# --- Configuration: list your tickers and their sectors here ---
# Some tickers appear in multiple sectors intentionally; using a list to allow duplicates.
TICKERS = [
    ("APP",   "AdTech"),
    ("APPS",  "AdTech"),
    ("CRTO",  "AdTech"),
    ("DV",    "AdTech"),
    ("GOOGL", "AdTech"),
    ("META",  "AdTech"),
    ("MGNI",  "AdTech"),
    ("PUBM",  "AdTech"),
    ("TTD",   "AdTech"),

    ("ACN",   "IT Services Legacy Tech"),
    ("CTSH",  "IT Services Legacy Tech"),
    ("DXC",   "IT Services Legacy Tech"),
    ("HPQ",   "IT Services Legacy Tech"),
    ("IBM",   "IT Services Legacy Tech"),
    ("INFY",  "IT Services Legacy Tech"),
    ("PLTR",  "IT Services Legacy Tech"),
    ("WIT",   "IT Services Legacy Tech"),

    ("AMAT",  "Semiconductors"),
    ("AMD",   "Semiconductors"),
    ("ARM",   "Semiconductors"),
    ("AVGO",  "Semiconductors"),
    ("INTC",  "Semiconductors"),
    ("NVDA",  "Semiconductors"),
    ("QCOM",  "Semiconductors"),
    ("TSM",   "Semiconductors"),

    ("AMZN",  "Cloud Infrastructure"),
    ("CRM",   "Cloud Infrastructure"),
    ("CSCO",  "Cloud Infrastructure"),
    ("GOOGL", "Cloud Infrastructure"),
    ("MSFT",  "Cloud Infrastructure"),
    ("NET",   "Cloud Infrastructure"),
    ("ORCL",  "Cloud Infrastructure"),
    ("SNOW",  "Cloud Infrastructure"),

    ("AAPL",  "Hardware Devices"),
    ("DELL",  "Hardware Devices"),
    ("HPQ",   "Hardware Devices"),
    ("LOGI",  "Hardware Devices"),
    ("PSTG",  "Hardware Devices"),
    ("SMCI",  "Hardware Devices"),
    ("SSYS",  "Hardware Devices"),
    ("STX",   "Hardware Devices"),
    ("WDC",   "Hardware Devices"),

    ("CCCS",  "Vertical SaaS"),
    ("CPRT",  "Vertical SaaS"),
    ("CSGP",  "Vertical SaaS"),
    ("GWRE",  "Vertical SaaS"),
    ("ICE",   "Vertical SaaS"),
    ("PCOR",  "Vertical SaaS"),
    ("SSNC",  "Vertical SaaS"),
    ("TTAN",  "Vertical SaaS"),

    ("AFRM",  "FinTech"),
    ("BILL",  "FinTech"),
    ("COIN",  "FinTech"),
    ("FIS",   "FinTech"),
    ("FI",    "FinTech"),
    ("GPN",   "FinTech"),
    ("PYPL",  "FinTech"),
    ("SSNC",  "FinTech"),
    ("XYZ",   "FinTech"),

    ("CHKP",  "Cybersecurity"),
    ("CRWD",  "Cybersecurity"),
    ("CYBR",  "Cybersecurity"),
    ("FTNT",  "Cybersecurity"),
    ("NET",   "Cybersecurity"),
    ("OKTA",  "Cybersecurity"),
    ("PANW",  "Cybersecurity"),
    ("S",     "Cybersecurity"),
    ("ZS",    "Cybersecurity"),

    ("ADSK",  "Enterprise SaaS"),
    ("AMZN",  "Enterprise SaaS"),
    ("CRM",   "Enterprise SaaS"),
    ("IBM",   "Enterprise SaaS"),
    ("MSFT",  "Enterprise SaaS"),
    ("NOW",   "Enterprise SaaS"),
    ("ORCL",  "Enterprise SaaS"),
    ("SAP",   "Enterprise SaaS"),
    ("WDAY",  "Enterprise SaaS"),

    ("BABA",  "eCommerce"),
    ("BKNG",  "eCommerce"),
    ("CHWY",  "eCommerce"),
    ("EBAY",  "eCommerce"),
    ("ETSY",  "eCommerce"),
    ("PDD",   "eCommerce"),
    ("SE",    "eCommerce"),
    ("SHOP",  "eCommerce"),
    ("WMT",   "eCommerce"),

    ("DDOG",  "Dev Tools Analytics"),
    ("ESTC",  "Dev Tools Analytics"),
    ("GTLB",  "Dev Tools Analytics"),
    ("MDB",   "Dev Tools Analytics"),
    ("TEAM",  "Dev Tools Analytics"),

    ("ADBE",  "SMB SaaS"),
    ("GOOGL", "SMB SaaS"),
    ("HUBS",  "SMB SaaS"),
    ("INTU",  "SMB SaaS"),
    ("META",  "SMB SaaS"),

    ("ABNB",  "Consumer Internet"),
    ("NFLX",  "Consumer Internet"),
    ("PINS",  "Consumer Internet"),
    ("SNAP",  "Consumer Internet"),
    ("SPOT",  "Consumer Internet"),
    ("TRIP",  "Consumer Internet"),
    ("YELP",  "Consumer Internet"),

    ("AMZN",  "AI Infrastructure"),
    ("IBM",   "AI Infrastructure"),
    ("NVDA",  "AI Infrastructure"),
    ("ORCL",  "AI Infrastructure"),
]

# --- Helper to retrieve total shares outstanding ---
def share_count(ticker: str) -> int:
    """
    Return the number of shares outstanding for `ticker`.
    Tries Finnhub first, then falls back to yfinance.
    """
    data = FINN.company_basic_financials(ticker, 'all')
    metric = data.get("metric", {}) or {}
    shares = metric.get("shareOutstanding") or metric.get("sharesOutstanding")
    if not shares:
        t = yf.Ticker(ticker)
        shares = (
            t.fast_info.get("sharesOutstanding") or
            t.info.get("sharesOutstanding")
        ) or 0
    try:
        return int(shares)
    except Exception:
        return 0

# --- Main ETL ---
def main(days_back: int = 30):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    engine = sqlalchemy.create_engine(db_url)
    
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=days_back)

    rows = []
    for ticker, sector in TICKERS:
        df = yf.download(
            ticker,
            start=start_date.isoformat(),
            end=(end_date + dt.timedelta(days=1)).isoformat(),
            progress=False,
            auto_adjust=False
        )
        if df.empty:
            print(f"⚠️ no price data for {ticker}")
            continue

        scount = share_count(ticker)
        df = df.reset_index().rename(columns={"Date": "date", "Close": "close_price"})
        df["ticker"] = ticker
        df["sector"] = sector
        df["shares_outstanding"] = scount
        # Compute market cap per row
        df["market_cap"] = df.apply(lambda r: r["close_price"] * r["shares_outstanding"], axis=1)
        rows.append(df[["date", "ticker", "sector", "close_price", "shares_outstanding", "market_cap"]])

    if not rows:
        print("❌ No market cap records to insert")
        return

    all_data = pd.concat(rows)
    all_data.to_sql(
        "market_caps",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✅ Inserted {len(all_data)} rows into market_caps table.")

if __name__ == "__main__":
    main()
