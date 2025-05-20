# market_cap_loader.py
# -------------------------------------------
# Fetch 30 business days of market-cap history for each stock in your 14 sectors
# and load into the Postgres table `market_cap_history` (no CSVs involved).

import os
from datetime import date
import pandas as pd
yfinance_installed=True
import yfinance as yf
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine

# 1) Define your 14 sectors & their tickers
#    Tickers may appear in multiple sectors; that’s acceptable.
TICKERS_BY_SECTOR = {
    "AdTech": ["APP","APPS","CRTO","DV","GOOGL","META","MGNI","PUBM","TTD"],
    "IT Services / Legacy Tech": ["ACN","CTSH","DXC","HPQ","IBM","INFY","PLTR","WIT"],
    "Semiconductors": ["AMAT","AMD","ARM","AVGO","INTC","NVDA","QCOM","TSM"],
    "Cloud Infrastructure": ["AMZN","CRM","CSCO","GOOGL","MSFT","NET","ORCL","SNOW"],
    "Hardware Devices": ["AAPL","DELL","HPQ","LOGI","PSTG","SMCI","SSYS","STX","WDC"],
    "Vertical SasS": ["CCCS","CPRT","CSGP","GWRE","ICE","PCOR","SSNC","TTAN"],
    "FinTech": ["AFRM","BILL","COIN","FIS","FI","GPN","PYPL","SSNC","XYZ"],
    "Cybersecurity": ["CHKP","CRWD","CYBR","FTNT","NET","OKTA","PANW","S","ZS"],
    "Enterprise SaaS": ["ADSK","AMZN","CRM","IBM","MSFT","NOW","ORCL","SAP","WDAY"],
    "eCommerce": ["AMZN","BABA","BKNG","CHWY","EBAY","ETSY","PDD","SE","SHOP","WMT"],
    "Dev Tools / Analytics": ["DDOG","ESTC","GTLB","MDB","TEAM"],
    "SMB SaaS": ["ADBE","BILL","GOOGL","HUBS","INTU","META"],
    "Consumer Internet": ["ABNB","BKNG","GOOGL","META","NFLX","PINS","SNAP","SPOT","TRIP","YELP"],
    "AI Infrastructure": ["AMZN","GOOGL","IBM","META","MSFT","NVDA","ORCL"]
}

# 2) Determine the date range: last 30 business days
today      = pd.to_datetime(date.today())
start_date = today - BDay(30)

# 3) Connect to Postgres using the DATABASE_URL environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
engine = create_engine(DATABASE_URL)

# 4) Fetch market-cap for each ticker on each business day
records = []
for sector, tickers in TICKERS_BY_SECTOR.items():
    for symbol in tickers:
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, end=today)["Close"]
        shares = tk.info.get("sharesOutstanding")
        if not shares:
            print(f"⚠️ No sharesOutstanding for {symbol}, skipping.")
            continue
        for dt, close in hist.items():
            records.append({
                "date":       dt.date(),
                "ticker":     symbol,
                "sector":     sector,
                "market_cap": close * shares
            })

# 5) Persist into market_cap_history table (replacing any existing table)
df = pd.DataFrame(records)
with engine.begin() as conn:
    conn.execute("DROP TABLE IF EXISTS market_cap_history;")
    conn.execute(
        "CREATE TABLE market_cap_history (date DATE, ticker TEXT, sector TEXT, market_cap DOUBLE PRECISION);")
    df.to_sql("market_cap_history", conn, if_exists="append", index=False)

print(f"Inserted {len(df)} rows into market_cap_history")
